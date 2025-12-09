import smtplib
from email.message import EmailMessage

FROM = "jjs7199@psu.edu"          # your PSU email address
TO = "jjs7199@psu.edu"            # send test email to yourself
SUBJECT = "UGSurvey SMTP TEST from ETL server"

msg = EmailMessage()
msg["From"] = FROM
msg["To"] = TO
msg["Subject"] = SUBJECT
msg.set_content("This is a manual SMTP test sent from the ETL server.")

print("Connecting to smtp.psu.edu:25 ...")
with smtplib.SMTP("smtp.psu.edu", 25) as smtp:
    smtp.set_debuglevel(1)   # show full SMTP conversation
    smtp.send_message(msg)

print("Sent.")
