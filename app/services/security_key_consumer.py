import pika
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from app.config import settings


def send_security_key_email(email: str, code: str):
    """Función para enviar el correo electrónico con la clave de seguridad usando SendGrid"""

    # ID de la plantilla dinámica
    template_id = "d-9fce5a2cd717486995e8cc8c3249178b"

    message = Mail(
        from_email="jhonier.1701814263@ucaldas.edu.co",
        to_emails=email,
    )

    # Usamos la plantilla dinámica
    message.template_id = template_id

    # Datos dinámicos que se pasan a la plantilla
    dynamic_template_data = {"code": code}
    message.dynamic_template_data = dynamic_template_data
    try:
        sg = SendGridAPIClient(settings.SENDGRID)
        sg.send(message)
    except Exception as e:
        print(e)


def callback(ch, method, properties, body):
    """Callback para procesar los mensajes de RabbitMQ"""
    message = json.loads(body)
    email = message["email"]
    code = message["security_key"]
    send_security_key_email(email, code)


def start_consumer():
    """Iniciar el consumidor de RabbitMQ"""
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="security_key_queue")

    channel.basic_consume(
        queue="security_key_queue", on_message_callback=callback, auto_ack=True
    )
    channel.start_consuming()
