
import datetime
import json
import math
import mysql.connector
import random
import statistics
import time


def get_names():
    s = """1	Noah	182,896	Emma	194,667
    2	Liam	173,636	Olivia	184,192
    3	Jacob	162,876	Sophia	180,832
    4	William	159,600	Isabella	170,185
    5	Mason	157,591	Ava	155,546
    6	Ethan	148,857	Mia	128,824
    7	Michael	144,741	Abigail	118,475
    8	Alexander	141,833	Emily	117,450
    9	James	139,307	Charlotte	102,269
    10	Elijah	136,649	Madison	98,258
    11	Benjamin	134,226	Elizabeth	93,787
    12	Daniel	133,595	Amelia	93,620
    13	Aiden	129,374	Evelyn	86,845
    14	Logan	126,600	Ella	85,760
    15	Jayden	125,796	Chloe	85,122
    16	Matthew	124,808	Harper	85,047
    17	Lucas	118,453	Avery	82,806
    18	David	116,382	Sofia	82,282
    19	Jackson	115,997	Grace	72,867
    20	Joseph	115,341	Addison	70,901
    21	Anthony	111,893	Victoria	70,872
    22	Samuel	107,854	Lily	68,153
    23	Joshua	105,978	Natalie	68,019
    24	Gabriel	105,296	Aubrey	65,809
    25	Andrew	104,830	Lillian	64,824
    26	John	102,521	Zoey	64,784
    27	Christopher	101,885	Hannah	64,327
    28	Oliver	97,962	Layla	63,062
    29	Dylan	96,213	Brooklyn	60,173
    30	Carter	95,047	Scarlett	58,174
    31	Isaac	94,338	Zoe	57,989
    32	Luke	93,102	Camila	57,827
    33	Henry	92,441	Samantha	56,854
    34	Owen	90,980	Riley	56,047
    35	Ryan	90,854	Leah	55,907
    36	Nathan	87,883	Aria	52,146
    37	Wyatt	87,572	Savannah	51,726
    38	Sebastian	86,535	Audrey	51,538
    39	Caleb	86,530	Anna	51,153
    40	Jack	85,569	Allison	48,941
    41	Christian	83,967	Gabriella	48,010
    42	Jonathan	79,687	Claire	47,673
    43	Julian	79,379	Hailey	47,671
    44	Landon	79,187	Penelope	47,526
    45	Levi	77,725	Aaliyah	46,584
    46	Isaiah	75,997	Sarah	46,507
    47	Hunter	74,698	Nevaeh	44,805
    48	Aaron	70,340	Kaylee	44,518
    49	Charles	69,859	Stella	44,143
    50	Thomas	69,850	Mila	44,040
    51	Eli	69,687	Nora	44,015
    52	Jaxon	68,978	Ellie	43,150
    53	Connor	68,183	Bella	43,034
    54	Nicholas	66,668	Alexa	41,966
    55	Jeremiah	66,545	Lucy	41,949
    56	Grayson	66,235	Arianna	41,708
    57	Cameron	66,081	Violet	41,484
    58	Adrian	65,876	Ariana	41,247
    59	Brayden	65,867	Genesis	40,449
    60	Evan	64,681	Alexis	40,303
    61	Jordan	64,287	Eleanor	40,055
    62	Josiah	62,869	Maya	39,676
    63	Angel	62,845	Caroline	39,579
    64	Robert	62,482	Peyton	39,557
    65	Gavin	62,267	Skylar	39,325
    66	Tyler	59,234	Madelyn	39,147
    67	Austin	58,989	Serenity	38,722
    68	Colton	58,358	Kennedy	38,527
    69	Jose	55,100	Taylor	38,161
    70	Dominic	55,062	Alyssa	37,854
    71	Brandon	54,030	Autumn	37,585
    72	Ian	52,402	Paisley	37,476
    73	Lincoln	51,925	Ashley	37,187
    74	Hudson	51,395	Brianna	36,846
    75	Kevin	51,243	Sadie	36,504
    76	Zachary	51,033	Naomi	36,083
    77	Adam	50,592	Kylie	36,044
    78	Mateo	50,495	Julia	35,855
    79	Jason	50,488	Sophie	35,605
    80	Chase	50,217	Mackenzie	35,217
    81	Nolan	49,994	Eva	35,146
    82	Ayden	49,535	Gianna	34,611
    83	Cooper	49,194	Luna	34,087
    84	Parker	49,146	Katherine	34,086
    85	Xavier	48,574	Hazel	33,834
    86	Asher	48,376	Khloe	33,752
    87	Carson	47,924	Ruby	33,368
    88	Jace	47,353	Melanie	33,046
    89	Easton	46,720	Piper	33,025
    90	Justin	45,493	Lydia	32,630
    91	Leo	44,591	Aubree	32,586
    92	Bentley	43,785	Madeline	32,334
    93	Jaxson	42,259	Aurora	32,024
    94	Nathaniel	41,773	Faith	31,847
    95	Blake	41,654	Alexandra	30,926
    96	Elias	40,837	Alice	30,575
    97	Theodore	40,416	Kayla	30,232
    98	Kayden	40,199	Jasmine	29,296
    99	Luis	39,026	Maria	28,910
    100	Tristan	38,605	Annabelle	28,901"""

    lines = s.split("\n")
    males = []
    females = []
    for line in lines:
        field = line.split("\t")

        m = field[1]
        f = field[3]

        males.append(m)
        females.append(f)

    return males, females


def create_user(id, name):
    sql = f'''insert into activities.users 
                (id, timestamp, full_name, has_image, generated) 
                values 
                (\'{id}\', {int(time.time() * 1000)}, \'{name}\', 0, 1)
            ;'''
    print(sql)


def create_session(id, start, end):
    sql = f"""INSERT INTO activities.sessions
                (activity_id, user_id, 
                start_timestamp, last_timestamp, end_timestamp, 
                feedback_score, concurrent_sessions)
                VALUES('meditation', '{id}', 
                {start}, {end}, {end}, 
            0, 0)
            ;
            """
    # print(sql)
    return sql


def init_session(id, mean_duration, duration_standard_deviation, mean_timestamp, timestamp_standard_deviation):
    # create_user(id, name)
    start = 0
    back = int(time.time() - 24 * 60 * 60)
    recent = int(time.time() - 45 * 60)
    while start < back or start > recent:
        start = int(random.normalvariate(mean_timestamp, timestamp_standard_deviation))

    duration = 0
    while duration < 1 or duration > 45:
        duration = int(random.normalvariate(mean_duration, duration_standard_deviation))

    start = start * 1000
    end = start + duration * 60 * 1000
    return create_session(id, start, end)


def bell(min, max):
    random_durations = []
    for i in range(200):
        r = random.randint(min, max)
        random_durations.append(r)
    mean = statistics.mean(random_durations)
    print(mean)
    variance = 0
    for i in range(200):
        variance += (mean - random_durations[i]) ** 2
    variance = int(variance / 200)
    print(variance)
    standard_deviation = math.sqrt(variance)
    print(standard_deviation)
    return mean, standard_deviation


def lambda_handler(event, context):

    males, females = get_names()

    mean_duration, duration_standard_deviation = bell(1, 45)
    mean_timestamp, timestamp_standard_deviation = bell(int(time.time() - 24 * 60 * 60), int(time.time()))

    all_sql = ''
    for name in males:
        id = f'g-m-{name.lower()}'
        all_sql += init_session(id, mean_duration, duration_standard_deviation, mean_timestamp, timestamp_standard_deviation)

    for name in females:
        id = f'g-f-{name.lower()}'
        all_sql += init_session(id, mean_duration, duration_standard_deviation, mean_timestamp, timestamp_standard_deviation)

    # f = open('sessions.sql', 'w')
    # f.write(all_sql)
    # f.close()

    with open("conf.json") as f:
        configuration = json.loads(f.read())

    c = mysql.connector.connect(
        host="activities-public.cb1k56tyiwoo.us-east-1.rds.amazonaws.com",
        user=configuration["mysql"]["user"],
        passwd=configuration["mysql"]["password"]
    )

    cursor = c.cursor(dictionary=True)
    # cursor.execute(all_sql, multi=True)
    queries = all_sql.split(";")
    for q in queries:
        if len(q.strip()) > 1:
            print(q)
            cursor.execute(q)
            c.commit()

    c.close()

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

# lambda_handler(None, None)
