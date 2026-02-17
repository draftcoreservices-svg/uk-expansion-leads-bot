import smtplib
from email.message import EmailMessage
from email.utils import formatdate


def send_email_smtp(*, host: str, port: int, user: str, password: str, mail_from: str, mail_to: str,
                    subject: str, html: str, attachment_name: str, attachment_csv: str):
    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Date"] = formatdate(localtime=False)
    msg["Subject"] = subject
    msg.set_content("This email contains an HTML report. If you cannot view it, please use an HTML-capable mail client.")
    msg.add_alternative(html, subtype="html")
    msg.add_attachment(attachment_csv.encode("utf-8"), maintype="text", subtype="csv", filename=attachment_name)

    with smtplib.SMTP(host, port, timeout=30) as s:
        s.starttls()
        if user:
            s.login(user, password)
        s.send_message(msg)
