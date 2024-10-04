import telebot
import pandas as pd
from telebot import types
from write_db import write,num_azs, write_1, current_month,last_month, df_tables, df_tbl_dog
from connectTW import engine
import time
import datetime
from datetime import date
import sqlalchemy
#import locale
from token1 import token_bot, id_support,id_IL,psw,to_email
import schedule
import threading
from sqlalchemy import MetaData, Table, String, Integer, Column, Text, DateTime, Boolean, Update, text, select, desc, insert, update, and_
from send_mail import send_email
import openpyxl
from openpyxl.styles import (
                        PatternFill, Font
                        )
from send_mail import send_email
import __main__
import re
#import os
import os,certifi
os.environ['SSL_CERT_FILE'] = certifi.where()


#locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
p = re.compile(r"^[0-9]+$", re.S)
#bot = telebot.TeleBot(token_bot)
bot = telebot.TeleBot(token_bot)
metadata = sqlalchemy.MetaData()

date_now = time.gmtime()
date_IL = date(date_now.tm_year,date_now.tm_mon, date_now.tm_mday)
date_IL_bot = date_IL.strftime('%Y.%m.%d')
date_itog = date(date_now.tm_year,date_now.tm_mon, 1)
date_itog_bot = date_itog.strftime('%Y.%m.%d')

# def start_polling():
#     bot.infinity_polling(none_stop=True)

# polling_thread = threading.Thread(target=start_polling)
# polling_thread.start()
#переменные для ввода данных ЭЭ
value = ''
old_value = ''

#клавиатура для ввода данных ЭЭ
markup = types.InlineKeyboardMarkup()
button_7 = types.InlineKeyboardButton('7',callback_data='7',row_width=1)
button_8 = types.InlineKeyboardButton('8',callback_data='8')
button_9 = types.InlineKeyboardButton('9',callback_data='9')
markup.row(button_7,button_8,button_9)
button_4 = types.InlineKeyboardButton('4',callback_data='4')
button_5 = types.InlineKeyboardButton('5',callback_data='5')
button_6 = types.InlineKeyboardButton('6',callback_data='6')
markup.row(button_4,button_5,button_6)
button_1 = types.InlineKeyboardButton('1',callback_data='1')
button_2 = types.InlineKeyboardButton('2',callback_data='2')
button_3 = types.InlineKeyboardButton('3',callback_data='3')
markup.row(button_1,button_2,button_3)
button_del = types.InlineKeyboardButton('<=',callback_data='del')
button_0 = types.InlineKeyboardButton('0',callback_data='0')
button_OK = types.InlineKeyboardButton('Отправить',callback_data='OK')
markup.row(button_del,button_0,button_OK)

#обработчик сигнала start
@bot.message_handler(commands=['start'])
def start_message(message):
    #global ls_id
    try:
        with engine.connect() as conn:
            (f"""UPDATE `Договор`
                SET Инд_телеграм = {message.chat.id}
                WHERE Номер_АЗС = '{num_azs(message.from_user.first_name)}'
                AND Инд_телеграм = '';""", conn)
            conn.commit()
            # tbl = Table('Договор', metadata,autoload_with=engine)
            # print(tbl)
            # stmt = update(tbl).values(Инд_телеграм = {message.chat.id}).where(and_(tbl.columns.Номер_АЗС == {num_azs(message.from_user.first_name)}, tbl.columns.Инд_телеграм == ''))
            # result = conn.execute(stmt).fetchall()


            #print(result)


        # (f"""UPDATE `Договор`
        #                 SET Инд_телеграм = {message.chat.id}
        #                 WHERE Номер_АЗС = '{num_azs(message.from_user.first_name)}' AND Инд_телеграм = '';""",engine)
        #engine.commit()
        df_dog = df_tbl_dog()
        ls_id = [value for value in df_dog['Инд_телеграм'].to_list() if value]
        if str(message.from_user.id) in ls_id:
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
            button_1 = types.KeyboardButton(text='/start')
            markup.row(button_1)
            button_2 = types.KeyboardButton(text = 'Передать показания')
            button_3 = types.KeyboardButton(text = 'Открыть журнал')
            markup.row(button_2, button_3)
            #bot.send_message(message.chat.id, first_mess,parse_mode='html',reply_markup=markup)
            bot.send_message(message.chat.id,f'<b>{message.from_user.first_name}</b>, здравствуйте! Авторизация прошла успешно. Можете передать показания\n',parse_mode='html',reply_markup=markup)
            bot.send_message(id_support, f'<b>{message.from_user.first_name}</b>, авторизовался',parse_mode='html')
            #bot.register_next_step_handler(message, on_click)
        else:
            bot.send_message(message.chat.id,f'<b>{message.from_user.first_name}</b>, здравствуйте! Ваш аккаунт не авторизован. Вход запрещён\n',parse_mode='html')
    except IndexError: #sqlite3.OperationalError:
        bot.send_message(message.chat.id,f'<b>{message.from_user.first_name}</b>, здравствуйте! Авторизация запрещена.\n',parse_mode='html')



#обработчик сигнала с ReplyKeyboard
@bot.message_handler(content_types=["text"])
def handle_text(message):
    #global value
    if message.text == 'Передать показания':
        data_set(message.chat.id)
    elif message.text == 'Открыть журнал':
        journal(message)
########################################################################
    elif p.search(message.text):
        print(message.chat.id)
        bot.send_message(message.chat.id, '<b>ЖДИТЕ, ОТПРАВЛЯЮ ИНФОРМАЦИЮ НА СЕРВЕР</b>', parse_mode='html')
        time.sleep(1.5)
        legal_date(message)
#############################################################################


#     print(message)
#     #global text

#     if datetime.date.fromtimestamp(message.date) < datetime.date(2024,5,22):
#         bot.send_message(message.chat.id,f'<b>{message.from_user.first_name}</b>, показание приборов можно будет отправлять с 25.05.2024\n',parse_mode='html')
#     else:
#         legal_date(message)

#обработчик информации при передаче данных ЭЭ
def legal_date(message):
    #global name_user,id_user,pokazaniya,date_otch,date_time_otch
    df_dog = df_tbl_dog()
    stop_time = datetime.time(7,3,0)
    print(stop_time)
    date_locale_stop = datetime.datetime.now().time()
    print(date_locale_stop)
    if date_locale_stop > stop_time:
        bot.send_message(message.chat.id,f'<b>ПОКАЗАНИЯ НЕ ПРИНЯТЫ. Показание необходимо передавать до 9 часов.</b>\n',parse_mode='html')
    else:
        ls_id = df_dog['Инд_телеграм'].to_list()
        try:
            if str(message.chat.id) in ls_id:
                name_user = message.chat.first_name
                id_user = message.chat.id
                pokazaniya = message.text
                date_otch=time.strftime('%Y.%m.%d',time.localtime(message.date))
                date_time_otch=time.strftime('%d.%m.%Y %H:%M:%S',time.localtime(message.date))
                text=write(id_user,pokazaniya,df_dog,date_otch,date_time_otch)
                if text[0]=='подтверждение':
                    expectation(message,text)
                elif text=='0':
                    bot.send_message(message.chat.id,f"""<b>{message.chat.first_name}</b>, показания прибора учёта приняты.
            В дальнейшем, когда у меня будет больше информации, в ответ на ваше сообщение я буду отправлять вам информацию о количестве кВт израсходованных за смену и
            инфрмацию о том, на сколько больше или меньше было израсходовано электроэнергии по сравнению с предыдущей сменой.\n""",parse_mode='html')
                else:
                    bot.send_message(message.chat.id,text,parse_mode='html')
            else:
                bot.send_message(message.chat.id,f'<b>{message.from_user.first_name}</b>, Вы не авторизованы. Вы не можете отправлять сообщения боту\n',parse_mode='html')
        except ValueError:
            bot.send_message(message.chat.id,f'<b>ПОКАЗАНИЯ НЕ ПРИНЯТЫ. Введённые показания не должны содержать буквы и символы. Только цифры.</b>\n',parse_mode='html')

# InlineKeyboard  определение ошибки при наборе оператором данных ЭЭ
def message_button(message,text,dict_name,dict_value):
    #dict_values[dict_name] = dict_value


        keyboard = types.InlineKeyboardMarkup()
        callback_button = types.InlineKeyboardButton(text="Продолжить", callback_data="test")
        callback_button_1 = types.InlineKeyboardButton(text="Ввести снова", callback_data="test2")
        keyboard.add(callback_button, callback_button_1)
        try:
            bot.send_message(message.chat.id,f'<b>ПОКАЗАНИЯ НЕ ПРИНЯТЫ. Проверьте правильность введённых данных</b>\nРасход электроэнергии за смену превышает среднестатистический, более чем в {int(text[1]/text[2])} раза.\n Если показание введены верно - нажмите кнопку "Продолжить"',parse_mode='html',reply_markup=keyboard)
            print('Превышены показания'+ message.from_user.first_name)
            set_timer(dict_name)
        except ZeroDivisionError:
            bot.send_message(message.chat.id,f'<b>Показания приняты</b>',parse_mode='html')
            write_1(dict_value['sutochn'],
                    dict_value['relevant_table'],
                    dict_value['available_tables_id'],
                    dict_value['_df_dog'],
                    dict_value['_pokazaniya'],
                    time.strftime('%Y.%m.%d',time.localtime()),
                    time.strftime('%d.%m.%Y %H:%M:%S',time.localtime()))

#формирует в режими ожидания  словарь с данными
def expectation(message,text):
    global dict_value
    #print(message)
    print(text[5].iloc[0]['Инд_телеграм'])

    dict_name = f"{text[5].iloc[0]['Инд_телеграм']}"
    dict_value = {'sutochn':text[1], 'relevant_table':text[3],'available_tables_id':text[4],'_df_dog':text[5],'_pokazaniya':text[6]}

    print(dict_value)

    message_button(message,text,dict_name,dict_value)

def set_timer(dict_name):
    global job
    job = schedule.every(10).seconds.do(beep,int(dict_name))


def beep(id):
    schedule.cancel_job(job)
    bot.send_message(id, 'Время ожидания вашего решения истекло.\n Введите показания ПЭУ ещё раз.',parse_mode='html')
    data_set(id)
    #bot.send_message(id, '0',parse_mode='html',reply_markup=markup)







def data_set(id):
    print(1000000000)
    #global value
    #value = str()
    #print(value)
    bot.send_message(id, '0',parse_mode='html',reply_markup=markup)



def journal(message):
    month = {'01':'январь', '02':'февраль', '03':'март', '04':'апрель', '05':'май', '06':'июнь', '07':'июль', '08':'август', '09':'сентябрь', '10':'октябрь', '11':'ноябрь','12':'декабрь'}
    today = datetime.date.today()
    second = today.strftime("%m")
    first = today.replace(day=1)
    month_button = first - datetime.timedelta(days=1)
    month_button = month_button.strftime("%m")


    keyboard_2 = types.InlineKeyboardMarkup()
    callback_button = types.InlineKeyboardButton(text=month[second], callback_data="btn1")
    callback_button_1 = types.InlineKeyboardButton(text= month[month_button], callback_data="btn2")
    keyboard_2.add(callback_button, callback_button_1)
    bot.send_message(message.chat.id,f'Выберите период для получении информации о переданных показаниях ПУ',parse_mode='html',reply_markup=keyboard_2)

@bot.callback_query_handler(func=lambda call: call.data.startswith('btn'))
def save_btn(call):
    code = call.data[-1]
    if code.isdigit():
        code = int(code)
    print('Нажата кнопка журнала', + code)
    df_dog = df_tbl_dog()
    #print(engine)
    ls_id = df_dog['Инд_телеграм'].to_list()
    #print(call.message.chat.id, ls_id)
    if str(call.message.chat.id) in ls_id:
        df_dog=df_dog.drop_duplicates(['Объект'], keep='last')
        df_dog=df_dog[df_dog['Инд_телеграм'].isin([str(call.message.chat.id)])]
        relevant_table=f"{df_dog.iloc[0]['Номер_договора']}_{df_dog.iloc[0]['Объект']}"
        #print(relevant_table_2)
    #message = call.message
    #chat_id = message.chat.id
        if code == 1:
           table=current_month(relevant_table)
        elif code == 2:
            table=last_month(relevant_table)

    text=table.to_string()
        #print(text)

    bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text,parse_mode='html')

# @bot.callback_query_handler(func=lambda call: call.data == 'Предыдущий месяц')
# def save_btn(call):
#     print(call)
#     df_dog = df_tbl_dog()
#     ls_id = df_dog['Инд_телеграм'].to_list()
#     #print(engine)
#     #print(call.message.chat.id, ls_id)
#     if str(call.message.chat.id) in ls_id:
#         df_dog=df_dog.drop_duplicates(['Объект'], keep='last')
#         df_dog=df_dog[df_dog['Инд_телеграм'].isin([str(call.message.chat.id)])]
#         relevant_table_2=f"{df_dog.iloc[0]['Номер_договора']}_{df_dog.iloc[0]['Объект']}"
#         #print(relevant_table_2)
#     #message = call.message
#     #chat_id = message.chat.id
#         table=last_month(relevant_table_2)

#         text=table.to_string()
#         #print(text)

#     bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text,parse_mode='html')

# @bot.callback_query_handler(func=lambda call: call.data.startswith('test'))
# def callback_inline(call,dict_value):
#         try:
#             #df_dog = df_tbl_dog()
#             text=write_1(dict_value['sutochn'],
#                 dict_value['relevant_table'],
#                 dict_value['available_tables_id'],
#                 dict_value['_df_dog'],
#                 dict_value['_pokazaniya'],
#                 time.strftime('%Y.%m.%d',time.localtime()),
#                 time.strftime('%d.%m.%Y %H:%M:%S',time.localtime()))
#             bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text,parse_mode='html')

#         except telebot.apihelper.ApiTelegramException:
#             pass









@bot.callback_query_handler(func=lambda call: True)
def callback_inline(call):
    global value, old_value
    data=call.data
    #value=''
    #print(value)
    print(data)

    if data=='test':
         schedule.cancel_job(job)
         #print(message_button(None,None,None,None,'inline'),40000)
         try:
            #df_dog = df_tbl_dog()
            text=write_1(dict_value['sutochn'],
                dict_value['relevant_table'],
                dict_value['available_tables_id'],
                dict_value['_df_dog'],
                dict_value['_pokazaniya'],
                time.strftime('%Y.%m.%d',time.localtime()),
                time.strftime('%d.%m.%Y %H:%M:%S',time.localtime()))
            #text= write_1()
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text,parse_mode='html')

         except telebot.apihelper.ApiTelegramException:
            pass
    elif  data=='test2':
        bot.send_message(call.message.chat.id, '0',parse_mode='html',reply_markup=markup)

    if data.isdigit():
       value+=data

    print(value, old_value)
    if value != old_value:
        if value == '':
           bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='0',reply_markup=markup)
        else:
           bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=value,reply_markup=markup)
    elif data == 'del':
            value=value[0:-1]
            #print(value, 44)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=value,reply_markup=markup)
    elif data == 'OK':
            print(call.message.chat.id)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text='<b>ЖДИТЕ, ОТПРАВЛЯЮ ИНФОРМАЦИЮ НА СЕРВЕР</b>', parse_mode='html')
            value=''
            #print(call.message)
            time.sleep(1.5)
            legal_date(call.message)
    old_value=value
    # try:
    #     text=write_1()
    #     bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=text,parse_mode='html')

    # except telebot.apihelper.ApiTelegramException:
    #     pass

def send_message():
    df_dog = df_tbl_dog()
    ls_id = [value for value in df_dog['Инд_телеграм'].to_list() if value]
    #print(df_dog)
    for i in ls_id:
        if i == '6456585499':

            bot.send_message(i, 'Не забудьте до 09:00 передать показания приборов учёта электроэнергии')


def send_info():
    df_dog = df_tbl_dog()
    ls_id = [value for value in df_dog['Инд_телеграм'].to_list() if value]
    for i in ls_id:
       #if i == '6456585499':
        # bot.send_message(i, f"""<b>У бота появились дополнительные возможности.</b>\nЕсли свернуть клавиатуру, появятся 3 клавиши:\n 1."ПЕРЕДАТЬ ПОКАЗАНИЯ"\n
        #                             Открывает специальную экранную клавиатуру с помощью которой передаются показания прибора учёта электроэнергии(ПУЭ). Возможность передачи показаний с помощью клавиатуры мессенджера сохраняется.\n
        #                             2."ОТКРЫТЬ ЖУРНАЛ"\n Открывает окно с двумя клавишами "ТЕКУЩИЙ МЕСЯЦ" и "ПРОШЕДШИЙ МЕСЯЦ", которые выводят на экран ежедневные показания ПУЭ за текущий и прошедший месяц, соответственно.\nТеперь, <b>но только по согласованию с начальником АЗС,</b> можно не вносить показания ПУЭ в рукописный журнал на АЗС, т.к. все
        #                             переданные показания будут сохраняться на сервере.\n 3."/start"\n Перезапуск бота.""", parse_mode='html')
        bot.send_message(i, f"""<b>У бота появились дополнительные возможности.</b>\nЕсли свернуть клавиатуру, появятся 3 клавиши:\n 1."ПЕРЕДАТЬ ПОКАЗАНИЯ"\n
                                    Открывает специальную экранную клавиатуру с помощью которой передаются показания прибора учёта электроэнергии(ПУЭ). Возможность передачи показаний с помощью клавиатуры мессенджера сохраняется.\n
                                    2."ОТКРЫТЬ ЖУРНАЛ"\n Открывает окно с двумя клавишами "ТЕКУЩИЙ МЕСЯЦ" и "ПРОШЕДШИЙ МЕСЯЦ", которые выводят на экран ежедневные показания ПУЭ за текущий и прошедший месяц, соответственно.\nТеперь, <b>но только по согласованию с начальником АЗС,</b> можно не вносить показания ПУЭ в рукописный журнал на АЗС, т.к. все
                                    переданные показания будут сохраняться на сервере.\n 3."/start"\n Перезапуск бота.""", parse_mode='html')


# def send_info_2():
#     tables = df_tables()
#     ls_no_data=()
#     with engine.connect() as conn:
#         for table in tables['TABLE_NAME'][:-1]:
#             tbl = Table(table, metadata, autoload_with=conn)
#             stmt = select(tbl).where(tbl.columns.Дата == {time.strftime('%Y.%m.%d',time.localtime())}).order_by(desc(tbl.columns.Дата)).limit(1)
#             result = conn.execute(stmt).fetchall()
#             print(result)
#             if result == []:
#                 ls_no_data += (table,)
#         print(ls_no_data)

#         res = [x for x in re.findall(r'([0-9]*)\w*.*', ls_no_data)]
#         print(res)


def no_data():
    tables = df_tables()
    ls_no_data=()
    with engine.connect() as conn:
        for table in tables['TABLE_NAME'][:-1]:
            tbl = Table(table, metadata, autoload_with=conn)
            stmt = select(tbl).where(tbl.columns.Дата == {time.strftime('%Y.%m.%d',time.localtime())}).order_by(desc(tbl.columns.Дата)).limit(1)
            result = conn.execute(stmt).fetchall()
            print(result)
            if result == []:
                ls_no_data += (table,)
        print(ls_no_data)
    #conn.close()

    if ls_no_data == ():
        bot.send_message(id_support, "Все передали")

    else:


        insert_row(ls_no_data)

###################################################################################
def insert_row(ls_no_data):
    #global stop_time
    with engine.connect() as conn:
        for table in ls_no_data:
            tbl = Table(table, metadata, autoload_with=conn)
            query = select(tbl).order_by(desc(tbl.columns.Дата)).limit(1)
            result = conn.execute(query).fetchall()
            print(result)

            stmt = insert(tbl).values(Показание=str(int(result[0][0])+1),
                                    Расход_за_сутки = '1',
                                    Дата=time.strftime('%Y.%m.%d',time.localtime()),
                                    Дата_время=time.strftime('%d.%m.%Y %H:%M:%S',time.localtime()),
                                    Плательщик=result[0][4],
                                    Способ=result[0][5],
                                    Авто='А')

            conn.execute(stmt)
            conn.commit()
    #stop_time = time.time
    bot.send_message(id_support, str(ls_no_data))

    #conn.close()



def message_IL():

    date_locale = time.strftime('%Y.%m.%d',time.localtime())
    date_now_2 = time.gmtime()
    date_IL = date(date_now_2.tm_year,date_now_2.tm_mon, date_now_2.tm_mday)
    date_IL_bot = date_IL.strftime('%Y.%m.%d')


    if date_locale == date_IL_bot:
            #conn=engine.connect()
        with engine.connect() as conn:
            query = (f"""SELECT Показание
                    FROM `74030631000953_Варна, Мостовой`
                    WHERE Дата = '{date_locale}'""")
            data = conn.execute(text(query)).fetchall()
            #conn.close()
            #print(data)
        date_IL_mes = date_IL.strftime('%d.%m.%Y')
        mes_text=f"{date_IL_mes}, {data[0][0]}, п.Варна, пер.Мостовой"
        for i in (id_IL,id_support):
            bot.send_message(i, mes_text)

    else:
        pass


##########################################################################################
def scrol():
    date_locale = time.strftime('%Y.%m.%d',time.localtime())
    if date_locale == date_itog_bot:
        tables = df_tables()
        df = pd.DataFrame()
        with engine.connect() as conn:
            for table in tables['TABLE_NAME'][:-1]:
                #print(table)
                query = (f"""SELECT*FROM `{table}`
                        WHERE Дата = '{date_locale}'
                        ORDER BY Дата DESC LIMIT 1""")
                data = conn.execute(text(query)).fetchall()
                df_data = pd.DataFrame(data)
                df_data = df_data.assign(profit=table)
                df=df._append(df_data,ignore_index = True )
        df.to_excel(f'file/{date_locale}.xlsx', index=False)
        file = f'file/{date_locale}.xlsx'
        send_email(to_email, date_locale, file)

    else:
        pass



if __name__ == '__main__':
    def start_polling():
        bot.infinity_polling(none_stop=True, timeout=123)

    polling_thread = threading.Thread(target=start_polling)
    polling_thread.start()
    schedule.every().day.at("05:00").do(send_message)
    #schedule.every().day.at("10:26").do(send_info_2)
    schedule.every().day.at("07:30").do(no_data)
    # schedule.every().day.at("21:12").do(send_message)
    #schedule.every().day.at("21:13").do(send_info)
    # schedule.every().day.at("21:14").do(no_data)
    #schedule.every().day.at("08:35").do(insert_row)
    schedule.every().day.at("08:00").do(message_IL)
    #schedule.every().day.at("09:05").do(scrol)
    ###########################################################
    while True:
        schedule.run_pending()
        time.sleep(1)
