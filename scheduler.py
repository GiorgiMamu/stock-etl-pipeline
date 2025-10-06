import schedule
import time
import sys
from datetime import datetime


from main import run_etl_pipeline

DEFAULT_RUN_TIME = "09:00"

def job():
    print(f"Scheduled job triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    success = run_etl_pipeline()

    if success:
        print("\nScheduled job completed successfully")
    else:
        print("\nScheduled job failed")

    print(f"\nNext run scheduled for: {DEFAULT_RUN_TIME} tomorrow\n")


def run_scheduler(run_time: str = DEFAULT_RUN_TIME, run_now: bool = False):
    print("ETL PIPELINE SCHEDULER")
    print(f"current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scheduled run time: {run_time} daily")
    print("\nPress Ctrl+C to stop the scheduler")

    if run_now:
        print("Running ETL pipeline immediately...")
        job()

    schedule.every().day.at(run_time).do(job)

    print(f"Scheduler started. Pipeline will run daily at {run_time}")
    print(f"next run: {schedule.next_run()}\n")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nScheduler stopped by user (Ctrl+C)")
        print("Graceful shutdown complete\n")


def print_usage():
    print("ETL Pipeline Scheduler")
    print("\nUsage:")
    print("  python scheduler.py                 Run with default time (09:00)")
    print("  - The script must keep running for scheduling to work")
    print("  - Press Ctrl+C to stop the scheduler")

def parse_arguments():
    run_time = DEFAULT_RUN_TIME
    run_now = False

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg in ['--help', '-h', 'help']:
            print_usage()
            sys.exit(0)

        elif arg == '--time':
            # Get next argument as time
            if i + 1 >= len(sys.argv):
                print("Error: --time requires a time argument (HH:MM)")
                print_usage()
                sys.exit(1)

            run_time = sys.argv[i + 1]

            try:
                datetime.strptime(run_time, "%H:%M")
            except ValueError:
                print(f"error: Invalid time format '{run_time}'")
                print(" time must be in HH:MM format (24-hour)")
                print_usage()
                sys.exit(1)

            i += 2

        elif arg == '--now':
            run_now = True
            i += 1

        else:
            print(f"Error: Unknown argument '{arg}'")
            print_usage()
            sys.exit(1)

    return run_time, run_now


def main():
    run_time, run_now = parse_arguments()
    run_scheduler(run_time, run_now)

if __name__ == "__main__":
    main()