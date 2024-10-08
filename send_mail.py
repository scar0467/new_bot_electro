import smtplib                                              # Импортируем библиотеку по работе с SMTP
import os                                                   # Функции для работы с операционной системой, не зависящие от используемой операционной системы
from pathlib import Path
import time

import mimetypes                                            # Импорт класса для обработки неизвестных MIME-типов, базирующихся на расширении файла
from email import encoders                                  # Импортируем энкодер
from email.mime.base import MIMEBase                        # Общий тип
from email.mime.text import MIMEText                        # Текст/HTML
from email.mime.image import MIMEImage                      # Изображения
from email.mime.audio import MIMEAudio                      # Аудио
from email.mime.multipart import MIMEMultipart              # Многокомпонентный объект

from token1 import email_addr, psw








def send_email(addr_to, msg_subj, files):

    addr_from = email_addr                         # Отправитель
    password  = psw                                  # Пароль

    msg = MIMEMultipart()                                   # Создаем сообщение
    msg['From']    = addr_from                              # Адресат
    msg['To']      = addr_to                                # Получатель
    msg['Subject'] = msg_subj                               # Тема сообщения

    #body = msg_text                                       # Текст сообщения
    #msg.attach(MIMEText( body,'plain'))                     # Добавляем в сообщение текст

    process_attachement(msg, files)

    #======== Этот блок настраивается для каждого почтового провайдера отдельно ===============================================
    server = smtplib.SMTP_SSL('mail.nic.ru', 465)        # Создаем объект SMTP
    #server.starttls()                                      # Начинаем шифрованный обмен по TLS
    #server.set_debuglevel(True)                            # Включаем режим отладки, если не нужен - можно закомментировать
    server.login(addr_from, password)             # Получаем доступ
    server.send_message(msg)                                # Отправляем сообщение
    server.quit()                                           # Выходим
    #==========================================================================================================================

def process_attachement(msg, files):                        # Функция по обработке списка, добавляемых к сообщению файлов
    attach_file(msg,files)
    # print(files)
    # for f in files:

        # if os.path.isfile(files):                               # Если файл существует
        #     attach_file(msg,files)                              # Добавляем файл к сообщению
        # elif os.path.exists(files):                             # Если путь не файл и существует, значит - папка
        #     dir = os.listdir(files)                             # Получаем список файлов в папке
        #     for file in dir:                                    # Перебираем все файлы и...
        #         attach_file(msg,files+"/"+file)                 # ...добавляем каждый файл к сообщению

def attach_file(msg, filepath):                           # Функция по добавлению конкретного файла к сообщению

        for i in range(6):

            time.sleep(.5)
            filename = os.path.basename(filepath)                   # Получаем только имя файла

            ctype, encoding = mimetypes.guess_type(filepath)        # Определяем тип файла на основе его расширения
            if i == 0:
                if ctype is None or encoding is not None:               # Если тип файла не определяется
                    ctype = 'application/octet-stream'                  # Будем использовать общий тип
                maintype, subtype = ctype.split('/', 1)                 # Получаем тип и подтип
            if i == 1:
                if maintype == 'text':                                  # Если текстовый файл
                    with open(filepath) as fp:                          # Открываем файл для чтения
                        file = MIMEText(fp.read(), _subtype=subtype)    # Используем тип MIMEText
                        fp.close()                                      # После использования файл обязательно нужно закрыть
            if i == 2:
                if maintype == 'image':                               # Если изображение
                    with open(filepath, 'rb') as fp:
                        file = MIMEImage(fp.read(), _subtype=subtype)
                        fp.close()
            if i == 3:
                if maintype == 'audio':                               # Если аудио
                    with open(filepath, 'rb') as fp:
                        file = MIMEAudio(fp.read(), _subtype=subtype)
                        fp.close()
                else:                                                   # Неизвестный тип файла
                    with open(filepath, 'rb') as fp:
                        file = MIMEBase(maintype, subtype)              # Используем общий MIME-тип
                        file.set_payload(fp.read())                     # Добавляем содержимое общего типа (полезную нагрузку)
                        fp.close()
                        encoders.encode_base64(file)                    # Содержимое должно кодироваться как Base64
                file.add_header('Content-Disposition', 'attachment', filename=filename) # Добавляем заголовки
                msg.attach(file)                                        # Присоединяем файл к сообщению
