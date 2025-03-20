"""
Kafka Monitoring Script

This script provides tools to monitor Kafka topics, consumer groups, and lag metrics.
It can be used to monitor the health of the streaming infrastructure.
"""

import os
import sys
import time
import json
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional

from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient, ConsumerGroupDescription, ListConsumerGroupsResult

# Add parent directory to path for imports
sys.path.append(str(Path(os.path.dirname(os.path.abspath(__file__))).parent))

# Import project modules
from src import config
from src.logging_utils import setup_logging

# Set up logging
logger = setup_logging(log_level=config.LOG_LEVEL)

def create_admin_client() -> AdminClient:
    """
    Create a Kafka AdminClient for topic and consumer group management.
    
    Returns:
        AdminClient: Configured Kafka admin client
    """
    kafka_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS
    }
    return AdminClient(kafka_config)

def list_topics(admin_client: Optional[AdminClient] = None) -> Dict:
    """
    List all topics in the Kafka cluster.
    
    Args:
        admin_client (AdminClient, optional): Kafka admin client
        
    Returns:
        Dict: Dictionary of topics and their metadata
    """
    logger.info("Listing Kafka topics")
    
    close_client = False
    if admin_client is None:
        admin_client = create_admin_client()
        close_client = True
    
    try:
        # Get metadata for all topics
        topics_metadata = admin_client.list_topics(timeout=10)
        
        # Format and return topics
        topics = {}
        for topic, metadata in topics_metadata.topics.items():
            topics[topic] = {
                'topic': topic,
                'partitions': len(metadata.partitions),
                'error': metadata.error.str() if metadata.error else None
            }
        
        logger.info(f"Found {len(topics)} Kafka topics")
        return topics
    
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        return {}

def list_consumer_groups(admin_client: Optional[AdminClient] = None) -> List[str]:
    """
    List all consumer groups in the Kafka cluster.
    
    Args:
        admin_client (AdminClient, optional): Kafka admin client
        
    Returns:
        List[str]: List of consumer group IDs
    """
    logger.info("Listing Kafka consumer groups")
    
    close_client = False
    if admin_client is None:
        admin_client = create_admin_client()
        close_client = True
    
    try:
        # List consumer groups
        groups_result = admin_client.list_consumer_groups()
        
        # Extract and return group IDs
        groups = [group.group_id for group in groups_result.valid]
        
        logger.info(f"Found {len(groups)} consumer groups")
        return groups
    
    except Exception as e:
        logger.error(f"Error listing consumer groups: {e}")
        return []

def get_consumer_group_info(
    group_id: str,
    admin_client: Optional[AdminClient] = None
) -> Dict:
    """
    Get detailed information about a consumer group.
    
    Args:
        group_id (str): Consumer group ID
        admin_client (AdminClient, optional): Kafka admin client
        
    Returns:
        Dict: Consumer group information
    """
    logger.info(f"Getting info for consumer group: {group_id}")
    
    close_client = False
    if admin_client is None:
        admin_client = create_admin_client()
        close_client = True
    
    try:
        # Describe consumer group
        group_desc = admin_client.describe_consumer_groups([group_id])
        
        if group_id not in group_desc:
            logger.warning(f"Consumer group {group_id} not found")
            return {}
        
        # Extract group info
        info = group_desc[group_id]
        
        # Format and return group info
        result = {
            'group_id': group_id,
            'state': info.state,
            'members': []
        }
        
        for member in info.members:
            member_info = {
                'member_id': member.member_id,
                'client_id': member.client_id,
                'assignments': []
            }
            
            if member.assignment:
                for topic, partitions in member.assignment.topic_partitions.items():
                    for partition in partitions:
                        member_info['assignments'].append({
                            'topic': topic,
                            'partition': partition
                        })
            
            result['members'].append(member_info)
        
        return result
    
    except Exception as e:
        logger.error(f"Error getting consumer group info: {e}")
        return {}

def get_consumer_lag(
    group_id: str,
    topics: Optional[List[str]] = None
) -> Dict:
    """
    Get consumer lag for a specific consumer group.
    
    Args:
        group_id (str): Consumer group ID
        topics (List[str], optional): List of topics to check, default all subscribed topics
        
    Returns:
        Dict: Consumer lag information by topic and partition
    """
    logger.info(f"Checking consumer lag for group: {group_id}")
    
    # Configure consumer
    consumer_config = {
        'bootstrap.servers': config.KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'enable.auto.commit': 'false'  # Don't commit offsets
    }
    
    try:
        # Create consumer
        consumer = Consumer(consumer_config)
        
        # Get assigned partitions
        if topics:
            consumer.subscribe(topics)
        
        # Poll to get assignment
        consumer.poll(1.0)
        assignment = consumer.assignment()
        
        if not assignment:
            logger.warning(f"No partitions assigned to consumer group {group_id}")
            return {}
        
        # Get lag for each partition
        lag_info = {}
        for tp in assignment:
            # Get committed offset
            committed = consumer.committed([tp], timeout=10)
            if not committed or not committed[0].offset:
                logger.warning(f"No committed offset for {tp.topic}:{tp.partition}")
                continue
            
            # Get end offset (latest message)
            end_offsets = consumer.get_watermark_offsets(tp, timeout=10)
            
            # Calculate lag
            if committed and end_offsets:
                lag = end_offsets[1] - committed[0].offset
                
                # Store in result dict
                if tp.topic not in lag_info:
                    lag_info[tp.topic] = {}
                
                lag_info[tp.topic][tp.partition] = {
                    'committed_offset': committed[0].offset,
                    'end_offset': end_offsets[1],
                    'lag': lag
                }
        
        return lag_info
    
    except Exception as e:
        logger.error(f"Error getting consumer lag: {e}")
        return {}
    
    finally:
        # Clean up consumer
        try:
            consumer.close()
        except:
            pass

def monitor_consumer_lag(
    group_id: str,
    topics: Optional[List[str]] = None,
    interval: int = 5,
    duration: int = 60
):
    """
    Monitor consumer lag continuously for a period of time.
    
    Args:
        group_id (str): Consumer group ID to monitor
        topics (List[str], optional): List of topics to monitor, default all subscribed topics
        interval (int): Interval between checks in seconds
        duration (int): Total monitoring duration in seconds
    """
    logger.info(f"Starting consumer lag monitoring for group {group_id}")
    logger.info(f"Monitoring interval: {interval}s, Duration: {duration}s")
    
    if topics:
        logger.info(f"Monitoring topics: {', '.join(topics)}")
    else:
        logger.info("Monitoring all subscribed topics")
    
    # Calculate number of iterations
    iterations = max(1, duration // interval)
    end_time = time.time() + duration
    
    for i in range(iterations):
        # Check if monitoring duration is complete
        if time.time() > end_time:
            break
        
        # Get lag info
        lag_info = get_consumer_lag(group_id, topics)
        
        # Print lag info
        logger.info(f"Consumer lag check {i+1}/{iterations} for group {group_id}")
        
        if not lag_info:
            logger.warning("No lag information available")
        else:
            total_lag = 0
            for topic, partitions in lag_info.items():
                for partition, info in partitions.items():
                    logger.info(f"  {topic}:{partition} - Lag: {info['lag']} messages")
                    total_lag += info['lag']
            
            logger.info(f"Total lag across all partitions: {total_lag} messages")
        
        # Wait for next interval if not the last iteration
        if i < iterations - 1 and time.time() < end_time:
            time.sleep(min(interval, end_time - time.time()))

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Kafka Monitoring Tools')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # List topics command
    list_topics_parser = subparsers.add_parser('list-topics', help='List Kafka topics')
    
    # List consumer groups command
    list_groups_parser = subparsers.add_parser('list-groups', help='List consumer groups')
    
    # Get consumer group info command
    group_info_parser = subparsers.add_parser('group-info', help='Get consumer group details')
    group_info_parser.add_argument(
        '--group',
        type=str,
        required=True,
        help='Consumer group ID'
    )
    
    # Get consumer lag command
    lag_parser = subparsers.add_parser('lag', help='Get consumer lag')
    lag_parser.add_argument(
        '--group',
        type=str,
        required=True,
        help='Consumer group ID'
    )
    lag_parser.add_argument(
        '--topics',
        type=str,
        nargs='+',
        help='Topics to check (default: all subscribed topics)'
    )
    
    # Monitor lag command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor consumer lag')
    monitor_parser.add_argument(
        '--group',
        type=str,
        required=True,
        help='Consumer group ID'
    )
    monitor_parser.add_argument(
        '--topics',
        type=str,
        nargs='+',
        help='Topics to monitor (default: all subscribed topics)'
    )
    monitor_parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Monitoring interval in seconds (default: 5)'
    )
    monitor_parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='Monitoring duration in seconds (default: 60)'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Execute the requested command
    if args.command == 'list-topics':
        topics = list_topics()
        print(json.dumps(topics, indent=2))
    
    elif args.command == 'list-groups':
        groups = list_consumer_groups()
        print(json.dumps(groups, indent=2))
    
    elif args.command == 'group-info':
        group_info = get_consumer_group_info(args.group)
        print(json.dumps(group_info, indent=2))
    
    elif args.command == 'lag':
        lag_info = get_consumer_lag(args.group, args.topics)
        print(json.dumps(lag_info, indent=2))
    
    elif args.command == 'monitor':
        monitor_consumer_lag(args.group, args.topics, args.interval, args.duration)
    
    else:
        # If no command or invalid command, print help
        print("Please specify a valid command. Use --help for more information.") 