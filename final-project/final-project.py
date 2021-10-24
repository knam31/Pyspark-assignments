#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from email.parser import Parser
from functools import reduce
import re
import time
from datetime import datetime, timezone, timedelta


def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday,
                      tms.tm_hour, tms.tm_min, tms.tm_sec,
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))


# /!\ All functions below must return an RDD object /!\

# T1: replace pass with your code
def extract_email_network(rdd):
    rdd_mail = rdd.map(lambda x: Parser().parsestr(x))
    rdd_full_email_tuples = rdd_mail.map(
        lambda x: (x.get('From'), map(
            lambda x: x.strip(), reduce(
                lambda a, b: f"{a},{b}", filter(
                    lambda y: y, [x.get('To'), x.get('Cc'), x.get('Bcc')]), '').split(',')), date_to_dt(x.get('Date')))
    )
    rdd_email_triples = rdd_full_email_tuples.flatMap(lambda email: [(email[0], rpt, email[2]) for rpt in email[1]])
    email_regex = r'^([a-zA-Z0-9\!\#\$\%\&\'*\+\-\/\=\?\^\_\`\{\|\}\~\.]+)@(([a-zA-Z0-9]+\.)*(?=.*[a-zA-Z])([a-zA-Z0-9]+))$'
    enron_email_regex = r'^([a-zA-Z0-9\!\#\$\%\&\'*\+\-\/\=\?\^\_\`\{\|\}\~\.]+)@(([a-zA-Z0-9]+\.)*enron.com)$'

    enron_valid_email = lambda s: True if re.compile(enron_email_regex).fullmatch(s) else False

    rdd_email_triples_enron = rdd_email_triples.filter(
        lambda x: enron_valid_email(x[0]) and enron_valid_email(x[1])).filter(lambda x: x[0] != x[1]
    )
    distinct_triples = rdd_email_triples_enron.distinct()
    return distinct_triples


# T2: replace pass with your code
def get_monthly_contacts(rdd):
    monthly_contacts = rdd.map(
        lambda x: (
            (x[0], x[2].strftime("%m/%Y")), x[1] # Replace datetime with MM/YYYY string, groupby ((sender, month), reciever)
        )
    ).distinct().groupByKey().mapValues(len).map( # Remove mails to reciever in the same month
        lambda x: (
            x[0][0], (x[0][1], x[1]) # Format to (sender, (month, emails))
        )
    ).reduceByKey(
            lambda x, y: x if x[1] > y[1] else y # Reduce keys with highest emails in  month
    ).map(lambda x: (
            x[0], x[1][0], x[1][1] # Format to (sender, month, emails)
        )
    ).sortBy(lambda x: (
            x[2], x[0] # Sort
        ), False
    )
    return monthly_contacts


# T3: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):
    weighted_network = rdd.filter(
        lambda x: drange[0] < x[2] < drange[1] if drange else True # Filter by drange if exists
    ).map(
        lambda x: (
            (x[0], x[1]), x[2] # Format to ((o, d), t)
        )
    ).groupByKey().mapValues(len).map( # Groupby (o, d) and get count of edges
        lambda x: (
            x[0][0], x[0][1], x[1] # Format to (o, d, w)
        )
    )
    return weighted_network


# T4.1: replace pass with your code
def get_out_degrees(rdd):
    out_degrees = rdd.flatMap(
        lambda x: [ # Flat map to create entry for origin and destination nodes
            (x[0], x[2]),
            (x[1], 0)
        ]
    ).reduceByKey(lambda a, b: a + b).map( # Reduce by Key adding weights
        lambda x: (
            x[1], x[0] # Format to (d, n)
        )
    ).sortBy(lambda x: x, False) # Sort descending
    return out_degrees


# T4.2: replace pass with your code
def get_in_degrees(rdd):
    in_degrees = rdd.flatMap(
        lambda x: [ # Flat map to create entry for origin and destination nodes
            (x[0], 0),
            (x[1], x[2])
        ]
    ).reduceByKey(lambda a, b: a + b).map( # Reduce by Key adding weights
        lambda x: (
            x[1], x[0] # Format to (d, n)
        )
    ).sortBy(lambda x: x, False) # Sort descending
    return in_degrees
