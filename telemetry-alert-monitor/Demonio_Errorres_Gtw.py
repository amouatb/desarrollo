import smtplib
from email.message import EmailMessage
import psycopg2
import select


def obtener_conexion():
    # Credenciales de conexión predefinidas
    host = "jaksol-flex-pg-server.postgres.database.azure.com"
    database = "jaksol"
    user = "postgresql"
    password = "jaksol.,2022psql"

    # Establecer la conexión
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    return conn



# Crear un cursor

conn = obtener_conexion()
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute("LISTEN nuevo_registro;")

print("Esperando notificaciones en el canal 'nuevo_registro'...")

while True:
    if select.select([conn], [], [], 5) == ([], [], []):
        print("Esperando...")
    else:
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            print("Notificación recibida:", notify.payload)
            enviar_email(destinatario, asunto, cuerpo)
            


# Detalles del correo
SMTP_SERVER = 'mail.jaksol.io'
SMTP_PORT = 465  # 465 para SSL, o 587 para TLS
EMAIL_ADDRESS = 'plataforma@jaksol.io'
EMAIL_PASSWORD = 'pl4t4f0rm4j4ks0l123'

# Función para enviar el correo
def enviar_email(destinatario, asunto, cuerpo):
    # Crear el mensaje
    msg = EmailMessage()
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = destinatario
    msg['Subject'] = asunto
    msg.set_content(cuerpo)

    # Conectar al servidor SMTP
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg) 

# Configura el destinatario y el contenido del mensaje
destinatario = 'amouat@jaksol.io'
asunto = 'Se ha detectado un nuevo registro en la tabla jaksol_039_log_error_gtw.'
cuerpo = 'Nuevo Registro Detectado con demonio'

# Llamar a la función para enviar el correo
#enviar_email(destinatario, asunto, cuerpo)

#############################