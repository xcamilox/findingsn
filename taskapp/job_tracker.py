from django.db import connection

class JobTracker():
    def getJobs(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM django_dramatiq_task order by created_at DESC")
            row = cursor.fetchall()

        return row