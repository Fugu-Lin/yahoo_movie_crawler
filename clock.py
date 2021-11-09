from apscheduler.schedulers.blocking import BlockingScheduler
import os
sched = BlockingScheduler()

@sched.scheduled_job('cron', day_of_week='mon-sun', hour=12)
def scheduled_job():
    print('This job is run every weekday at 5pm.')
    os.system("python Yahoo_Movie.py")
    os.system("python Yahoo_Movie_Comingsoon_2_Firebase.py")
sched.start()
