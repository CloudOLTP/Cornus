import sys
import smtplib

zhihan_send = "experiments.guo@gmail.com"
zhihan_recv = "zhihan@cs.wisc.edu"
zhihan_pass = 'expnotification@'
xinyu_send  = "exp.zeng@gmail.com"
xinyu_recv  = "xzeng56@wisc.edu"
xinyu_pass  = 'experiment'

sender = xinyu_send
recver = xinyu_recv
password = xinyu_pass

def send_email(msg):
    smtpObj = smtplib.SMTP('smtp.gmail.com',587)
    smtpObj.ehlo()
    smtpObj.starttls()
    smtpObj.login(sender, password)
    smtpObj.sendmail(sender, recver, "Subject: {} is done!".format(msg))
    smtpObj.quit()

if __name__ == "__main__":
    send_email(sys.argv[1])