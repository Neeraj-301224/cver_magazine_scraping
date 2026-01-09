"""
Master script to run ALL spiders sequentially, then process and insert events.

This script:
1. Runs all spiders from all categories one by one:
   - Community & Social: bhf, eventbrite, gosh, macmillan
   - Fitness & Training: findarace, letsdothis, runguides, runthrough, timeoutdoors, ukrunningevents
   - Wellness & Mind: mindfulnessassociation, mindfulnessuk, mindspace, pilatesflow, sharphamtrust, yogawithmanon
2. After all spiders complete, runs insert_event.py to process and insert events

Uses subprocess to run each spider in a separate process to avoid Twisted reactor conflicts.
"""
import sys
import subprocess
from datetime import datetime
from pathlib import Path

# Import insert_event module
from insert_event import main as insert_events


def get_spider_config(spider_class, spider_name):
    """Get configuration for a spider including output file path."""
    scraped_data_dir = Path(__file__).parent / "scraped_data"
    scraped_data_dir.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_file = str(scraped_data_dir / f"{spider_name}_{date_str}.json")
    
    return {
        "spider_class": spider_class,
        "spider_name": spider_name,
        "output_file": output_file
    }


def run_all_spiders():
    """Run all spiders sequentially, each in a separate subprocess to avoid Twisted reactor conflicts."""
    # Define all spiders from all categories
    spiders = [
        # Community & Social
        "bhf",
        "eventbrite",
        "gosh",
        "macmillan",
        # Fitness & Training
        "findarace",
        "letsdothis",
        "runguides",
        "runthrough",
        "timeoutdoors",
        "ukrunningevents",
        # Wellness & Mind
        "mindfulnessassociation",
        "mindfulnessuk",
        "mindspace",
        "pilatesflow",
        "sharphamtrust",
        "yogawithmanon",
    ]
    
    print("=" * 80)
    print("🚀 STARTING SPIDER EXECUTION")
    print("=" * 80)
    print(f"Total spiders to run: {len(spiders)}")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Get script directory to find individual spider scripts
    script_dir = Path(__file__).parent
    
    # Run each spider in a separate subprocess
    # This prevents Twisted reactor conflicts
    successful = 0
    failed = 0
    
    for i, spider_name in enumerate(spiders, 1):
        try:
            print(f"\n{'=' * 80}")
            print(f"[{i}/{len(spiders)}] Running: {spider_name}")
            print(f"{'=' * 80}")
            
            # Find the individual spider script
            spider_script = script_dir / f"run_{spider_name}_spider.py"
            
            if not spider_script.exists():
                print(f"❌ Spider script not found: {spider_script}")
                failed += 1
                continue
            
            print(f"Running script: {spider_script.name}")
            
            # Run the spider script in a subprocess
            result = subprocess.run(
                [sys.executable, str(spider_script)],
                cwd=str(script_dir),
                capture_output=False,  # Show output in real-time
                text=True
            )
            
            # Check if spider completed successfully
            if result.returncode == 0:
                # Check if output file was created
                scraped_data_dir = script_dir / "scraped_data"
                date_str = datetime.now().strftime("%Y-%m-%d")
                output_file = scraped_data_dir / f"{spider_name}_{date_str}.json"
                
                if output_file.exists():
                    file_size = output_file.stat().st_size
                    if file_size > 0:
                        print(f"✅ {spider_name} completed successfully - Output file: {output_file.name} ({file_size} bytes)")
                        successful += 1
                    else:
                        print(f"⚠️  {spider_name} completed but output file is empty")
                        failed += 1
                else:
                    print(f"⚠️  {spider_name} completed but no output file was created")
                    failed += 1
            else:
                print(f"❌ {spider_name} failed with return code: {result.returncode}")
                failed += 1
            
        except KeyboardInterrupt:
            print(f"\n⚠️  Process interrupted by user during {spider_name}")
            return False
        except Exception as e:
            print(f"❌ {spider_name} failed with exception: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
            continue
    
    print(f"\n{'=' * 80}")
    print("✅ ALL SPIDERS COMPLETED")
    print(f"{'=' * 80}")
    print(f"Successful: {successful}/{len(spiders)}")
    print(f"Failed: {failed}/{len(spiders)}")
    print(f"{'=' * 80}")
    
    return successful > 0


def main():
    """Main function to run all spiders and then insert events."""
    start_time = datetime.now()
    
    try:
        # Step 1: Run all spiders
        print("\n" + "=" * 80)
        print("STEP 1: RUNNING ALL SPIDERS")
        print("=" * 80)
        spiders_success = run_all_spiders()
        
        if not spiders_success:
            print("\n⚠️  Some spiders failed, but continuing with insertion process...")
        
        # Step 2: Insert events
        print("\n" + "=" * 80)
        print("STEP 2: PROCESSING AND INSERTING EVENTS")
        print("=" * 80)
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        insert_events()
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\n" + "=" * 80)
        print("🎉 COMPLETE PROCESS FINISHED")
        print("=" * 80)
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total duration: {duration}")
        print("=" * 80)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

