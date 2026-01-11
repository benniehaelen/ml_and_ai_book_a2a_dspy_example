#!/usr/bin/env python3
"""
Retail Analytics Assistant - Demo Runner
Demonstrates A2A + DSPy multi-agent system

Usage:
    python run_demo.py              # Run interactive demo
    python run_demo.py --standalone # Run without A2A servers
    python run_demo.py --servers    # Start A2A servers only
"""

import asyncio
import sys
import os
import argparse
import subprocess
import time
import signal
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def check_dependencies():
    """Check if required dependencies are installed"""
    missing = []
    
    try:
        import dspy
    except ImportError:
        missing.append("dspy-ai")
    
    try:
        import fastapi
    except ImportError:
        missing.append("fastapi")
    
    try:
        import uvicorn
    except ImportError:
        missing.append("uvicorn")
    
    try:
        import httpx
    except ImportError:
        missing.append("httpx")
    
    try:
        import pandas
    except ImportError:
        missing.append("pandas")
    
    if missing:
        print("❌ Missing dependencies:", ", ".join(missing))
        print("   Run: pip install -r requirements.txt")
        return False
    
    return True


def check_env():
    """Check environment configuration"""
    from dotenv import load_dotenv
    load_dotenv()
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("⚠️  Warning: OPENAI_API_KEY not found")
        print("   DSPy features will be limited without an API key")
        print("   Create a .env file with your API key")
        return False
    
    return True


def ensure_sample_data():
    """Ensure sample data exists"""
    db_path = os.getenv('DATABASE_PATH', 'data/retail_lakehouse.db')
    
    if not os.path.exists(db_path):
        print("📦 Creating sample data...")
        from utils.create_sample_data import create_sample_data
        stats = create_sample_data(db_path)
        print(f"   ✓ Created {stats['transactions']:,} transactions")
        print(f"   ✓ Created {stats['products']:,} products")
        print(f"   ✓ Created {stats['stores']} stores")
    else:
        print(f"✓ Database exists: {db_path}")


async def run_standalone_demo():
    """Run demo without A2A servers (simpler)"""
    print("\n" + "="*60)
    print("🚀 Retail Analytics Assistant - Standalone Demo")
    print("="*60 + "\n")
    
    from agents.orchestrator import StandaloneOrchestrator
    
    orchestrator = StandaloneOrchestrator()
    
    # Example queries
    queries = [
        "Why are sales declining for premium outdoor products in the Northeast region?",
        # "What are the top 10 selling products this month?",
        # "Show me revenue by region for the last 30 days",
    ]
    
    for question in queries:
        print(f"\n{'='*60}")
        print(f"❓ Question: {question}")
        print('='*60 + "\n")
        
        try:
            results = await orchestrator.process_query(question)
            
            print("📋 Analysis Plan:")
            print(f"   {results.get('plan', 'N/A')[:200]}...")
            
            print(f"\n📊 Tables Used: {', '.join(results.get('tables', []))}")
            
            print(f"\n💾 SQL Query:")
            sql = results.get('sql', 'N/A')
            for line in sql.split('\n')[:10]:
                print(f"   {line}")
            
            if results.get('row_count'):
                print(f"\n📈 Results: {results['row_count']} rows")
            
            print(f"\n💡 Answer:")
            answer = results.get('answer', 'N/A')
            print(f"   {answer[:500]}...")
            
            print(f"\n📌 Recommendations:")
            recs = results.get('recommendations', 'N/A')
            print(f"   {recs[:300]}...")
            
            print(f"\n🎯 Confidence: {results.get('confidence', 'N/A')}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*60)
    print("✅ Demo completed!")
    print("="*60)


def start_agent_servers():
    """Start A2A agent servers in separate processes"""
    processes = []
    
    agents = [
        ("Data Discovery Agent", "agents/data_discovery_agent.py", 8001),
        ("SQL Generation Agent", "agents/sql_generation_agent.py", 8002),
        ("Orchestrator Agent", "agents/orchestrator.py", 8000),
    ]
    
    def signal_handler(sig, frame):
        print("\n\n🛑 Shutting down agents...")
        for proc in processes:
            proc.terminate()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print("\n📦 Starting A2A Agent Servers...\n")
    
    for name, script, port in agents:
        print(f"   Starting {name} on port {port}...")
        proc = subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        processes.append(proc)
        time.sleep(2)  # Wait for server to start
    
    print("\n✅ All agents started!")
    print("\nAgent endpoints:")
    print("   • Orchestrator:    http://localhost:8000")
    print("   • Data Discovery:  http://localhost:8001")
    print("   • SQL Generation:  http://localhost:8002")
    print("\nAgent cards available at:")
    print("   • http://localhost:8000/.well-known/agent.json")
    print("   • http://localhost:8001/.well-known/agent.json")
    print("   • http://localhost:8002/.well-known/agent.json")
    print("\nPress Ctrl+C to stop all agents...")
    
    # Keep running until interrupted
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(None, None)


async def run_a2a_demo():
    """Run demo with A2A protocol"""
    print("\n" + "="*60)
    print("🚀 Retail Analytics Assistant - A2A Demo")
    print("="*60 + "\n")
    
    from utils.a2a_client import A2AClient
    
    async with A2AClient() as client:
        # Check if agents are running
        print("Checking agent health...")
        
        agents = [
            ("Orchestrator", "http://localhost:8000"),
            ("Data Discovery", "http://localhost:8001"),
            ("SQL Generation", "http://localhost:8002"),
        ]
        
        all_healthy = True
        for name, url in agents:
            healthy = await client.health_check(url)
            status = "✓" if healthy else "✗"
            print(f"   {status} {name}: {url}")
            if not healthy:
                all_healthy = False
        
        if not all_healthy:
            print("\n❌ Some agents are not running.")
            print("   Start agents with: python run_demo.py --servers")
            return
        
        print("\n✓ All agents healthy!\n")
        
        # Send test query
        question = "Why are sales declining for premium outdoor products in the Northeast?"
        print(f"❓ Question: {question}\n")
        
        result = await client.send_task(
            agent_url="http://localhost:8000",
            skill_id="answer-question",
            parameters={"question": question}
        )
        
        if result.get("status") == "completed":
            artifacts = result.get("artifacts", [])
            if artifacts:
                content = artifacts[0].get("content", {})
                
                print("📋 Answer:")
                print(f"   {content.get('answer', 'N/A')[:500]}...")
                
                print("\n📌 Recommendations:")
                print(f"   {content.get('recommendations', 'N/A')[:300]}...")
        else:
            print(f"❌ Task failed: {result.get('error', 'Unknown error')}")


def interactive_mode():
    """Run interactive query mode"""
    print("\n" + "="*60)
    print("🚀 Retail Analytics Assistant - Interactive Mode")
    print("="*60)
    print("\nType your questions and press Enter.")
    print("Type 'quit' or 'exit' to stop.\n")
    
    from agents.orchestrator import StandaloneOrchestrator
    orchestrator = StandaloneOrchestrator()
    
    while True:
        try:
            question = input("\n❓ Your question: ").strip()
            
            if question.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if not question:
                continue
            
            print("\n⏳ Processing...\n")
            results = asyncio.run(orchestrator.process_query(question))
            
            print("💡 Answer:")
            print(f"   {results.get('answer', 'N/A')}")
            
            print("\n📌 Recommendations:")
            print(f"   {results.get('recommendations', 'N/A')}")
            
            if results.get('sql'):
                print("\n💾 SQL Query:")
                print(f"   {results['sql'][:200]}...")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Retail Analytics Assistant Demo"
    )
    parser.add_argument(
        '--standalone', 
        action='store_true',
        help='Run standalone demo without A2A servers'
    )
    parser.add_argument(
        '--servers', 
        action='store_true',
        help='Start A2A servers only'
    )
    parser.add_argument(
        '--a2a', 
        action='store_true',
        help='Run demo using A2A protocol (requires servers)'
    )
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Run interactive query mode'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("   Retail Analytics Assistant")
    print("   A2A Protocol + DSPy Multi-Agent System")
    print("="*60)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    print("\n✓ Dependencies OK")
    
    # Check environment
    check_env()
    
    # Ensure sample data
    ensure_sample_data()
    
    # Run appropriate mode
    if args.servers:
        start_agent_servers()
    elif args.a2a:
        asyncio.run(run_a2a_demo())
    elif args.interactive:
        interactive_mode()
    else:
        # Default: standalone demo
        asyncio.run(run_standalone_demo())


if __name__ == "__main__":
    main()
