# scraping
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup as Soup
import requests
import datetime as dt
import yfinance as yf

# email
import smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def is_sensitive(x):
    # determine if announcement is market sensitive
    return x.find('img') is not None


def get_headline(x):
    # extract headline
    a = x.get_text()
    return a[:a.find('\n\n\n')].strip()


def to_datetime(x):
    # extract date and parse into datetime
    a = x.get_text().strip()
    date = a[0:10]
    time = a[11:]
    if len(time) < 8:
        time = '0' + time
    return pd.to_datetime(date + ' ' + time, format='%d/%m/%Y %I:%M %p')


def get_data(url):
    # get announcement data at url
    # return as dataframe

    page = requests.get(url).text  # get page html
    soup = Soup(page, features='html.parser')  # cook it
    announcements = soup.find('announcement_data').find_all('tr')  # find announcements table
    announcements.pop(0)  # remove headings row

    headings_announcement = ['Symbol', 'DateTime', 'Market Sensitive', 'Headline', 'Link']
    headings_price = ['High', 'Low', 'Open', 'Close']

    test = False  # setting to true means only 4 results are processed - to save time in testing
    test_counter = 0  # counter used in testing
    data = pd.DataFrame(data=None, columns=headings_announcement + headings_price)  # initialise empty df with headings
    for ann in announcements:
        # process announcements row-by-row
        # retrieve pricing data
        # append row to data df

        if (test_counter > 4) and test:  # only process 4 rows when testing
            break
        test_counter += 1

        # announcement info
        cols = ann.find_all('td')  # get column values for the row
        symbol = cols[0].get_text().strip()  # stock symbol
        ann_datetime = to_datetime(cols[1])  # announcement date
        ann_date = ann_datetime.date()  # announcement time
        new_row = pd.Series(dict(zip(headings_announcement,
                                     [symbol,
                                      ann_datetime,
                                      is_sensitive(cols[2]),
                                      get_headline(cols[3]),
                                      'https://www.asx.com.au' + cols[3].find('a').get('href')])))  # build new row

        # price info
        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        stock = yf.Ticker(symbol + '.AX')
        prices = stock.history(interval='1d', start=yesterday, end=today)[headings_price]  # get yesterday's prices
        if prices.size == 0:
            prices = pd.Series([np.nan] * len(headings_price), index=headings_price)  # nans if prices not found
        else:
            prices = prices.iloc[0]  # first row of data frame
        new_row = new_row.append(prices)  # append prices to new row series

        data = data.append(new_row, ignore_index=True)  # append new row to data dataframe

    # calculate price changes
    data['O-C change (%)'] = (((data['Close'] / data['Open']) - 1) * 100).round(2)
    data['H-L change (%)'] = (((data['High'] / data['Low']) - 1) * 100).round(2)

    # return and set column order
    return data[['Symbol', 'Open', 'Close', 'O-C change (%)', 'High', 'Low', 'H-L change (%)', 'DateTime',
                  'Market Sensitive', 'Headline', 'Link']]

# collect announcements
data = get_data('https://www.asx.com.au/asx/statistics/todayAnns.do')  # today's
data = data.append(get_data('https://www.asx.com.au/asx/statistics/prevBusDayAnns.do'))  # yesterday's
date_of_report = data['DateTime'].iloc[0].date()  # report date
filepath = '' ## add path
filename = 'asx_announcements_' + date_of_report.strftime('%Y-%m-%d') + '.csv'
data.to_csv(filepath + filename, index=False)  # save to csv


# send csv in an email
port = 465
smtp_server = "smtp.gmail.com"
subject = "ASX Announcements for morning of: " + date_of_report.strftime('%Y-%m-%d')
body = "Generated and sent: " + dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + \
       "\n\n May the odds be ever in your favour."
sender_email = '???@gmail.com'  # add gmail address
recipients_emails = []  ## add list of email addresses as strings
password = ''  # add password

# create a multipart message and set headers
message = MIMEMultipart()
message["From"] = sender_email
message['To'] = ",".join(recipients_emails)
message["Subject"] = subject

# add body to email
message.attach(MIMEText(body, "plain"))

# open pdf file in binary mode
with open(filepath + filename, "rb") as attachment:
    # add file as application/octet-stream
    # email client can usually download this automatically as attachment
    part = MIMEBase("application", "octet-stream")
    part.set_payload(attachment.read())

# encode file in ASCII characters to send by email
encoders.encode_base64(part)

# add header as key/value pair to attachment part
part.add_header(
    "Content-Disposition",
    f"attachment; filename= {filename}",
)

# add attachment to message and convert message to string
message.attach(part)
text = message.as_string()

# log in to server using secure context and send email
context = ssl.create_default_context()
with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, recipients_emails, text)
