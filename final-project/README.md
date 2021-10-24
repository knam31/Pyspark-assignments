# Pyspark-final-project

# Task 1: Basic Network Extraction (extract_email_network(rdd))

What to do: Write a Python function extract_email_network() that takes an RDD rdd as argument and returns an RDD of triples (S, R, T) each of which representing an Email transmission
from the sender S to the recipient R occurring at time T.
You can assume that rdd is obtained by calling the command below:
utf8_decode_and_filter (sc. sequenceFile ('<path_to_the_input_dataset >')))

Each Email message has a single sender and one or more recipients. The Email address of the
sender is the value of the From field in the message header, and the recipient Email addresses is
the union of the Email addresses in the To, Cc, and Bcc fields. The string representing the message
transmission timestamp is the value of the Date field. Thus, a single message may translate to
multiple output triples – one for each unique pair of the message sender and one of its recipients.
The RDD returned by extract_email_network() must satisfy all of the following constraints:
• Every Email address appearing in either the sender or the recipient field of every output
triple must be a valid Email address as per the definition in Assignment 2.
• Every Email address appearing in either the sender or the recipient field of every
output triple must belong to the enron.com domain, i.e., the last two labels of its
domain name must be enron followed by com. For example, jane.doe@enron.com
and joe.smart@sales.enron.com are both valid Enron Email addresses whereas both
joe.smart@senron.com and joe.smart@ibm.com are not.
• The timestamp field of every output triple must be an instance of the Python datetime
object. Use the provided method date_to_dt() to convert the string timestamp in the Date
field of the message header to an instance of datetime holding the equivalent time in the
UTC time zone.
• All self-loops, i.e., the triples having identical sender and recipient Email addresses, must be
excluded.
• All output triples must be distinct.


# Task 2: Computing Monthly Contacts (get_monthly_contacts(rdd))
In this task, you will write a function to compute, for each sender, the month where they contacted
the most people (assuming that one email address corresponds to one person).

What to do: Write a function get_monthly_contacts() that takes one argument rdd, which is
an RDD that complies with the output format of the function extract_email_network() specified
in Task 1. The function get_monthly_contacts() must return an RDD consisting of distinct
triples such that, for each triple (s, m, n)
• there is a triple (s, r, t) in the input RDD for some r and t,
• m is a string representing a month (in the format MM/YYYY),
• n is the number of people (i.e., distinct email addresses) who received an email from s during
month m,
• there is no other month (in the input dataset) where s sent strictly more than n emails (to
n distinct emails).
The output should be sorted in descending order on n first, then s

# Task 3: Creating a Weighted Network (convert_to_weighted_network(rdd, drange=None))

In this task, you will convert the Email network extracted in Task 1 to a weighted network in which
every two nodes are connected by at most two edges (one in either direction), and the weight of
each edge (a, b) is the number of Email messages sent from a to b.

What to do: Write a function convert_to_weighted_network() that takes one required
argument rdd, which is an RDD that complies with the output format of the function
extract_email_network() specified in Task 1, and one optional argument drange, which is
a pair (d1, d2) of datetime objects with a default value of None. The function returns an RDD
consisting of distinct triples (o, d, w) such that all of the following constraints hold:
• (o, d, t) is an element of the input RDD for some timestamp t;
• if drange is not None, then w is the number of edges (o', d', t) in the input RDD such
that (o', d') = (o, d) and drange[0] ≤ t ≤ drange[1];
• if drange is None, then w is the number of edges (o', d', t) in the input RDD such that 
(o', d') = (o, d).


# Task 4: Computing Basic Degree Statistics


## Task 4.1 (get_out_degrees(rdd))

Write a function get_out_degrees() that takes an RDD representing a weighted network as
argument, and returns an RDD of pairs (d, n) satisfying the constraints below:
• d is a non-negative integer;
• n is a string holding an Email address;
• the weighted out-degree of n is d;
• there is exactly one pair (d, n) for each node n in the input network (even if its weighted
out-degree is 0);
• the output is sorted in the descending lexicographical order of the integer/string pairs.


## Task 4.2 (get_in_degrees(rdd))

Write a function get_in_degrees() that takes an RDD representing a weighted network as argument, and returns an RDD of pairs (d, n) satisfying the constraints below:
• d is a non-negative integer;
• n is a string holding an Email address;
• the weighted in-degree of n is d;
• there is exactly one pair (d, n) for each node n in the input network (even if its weighted
in-degree is 0);
• the output is sorted in the descending lexicographical order of the integer/string pairs.