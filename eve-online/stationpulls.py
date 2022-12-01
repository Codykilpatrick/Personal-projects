import pandas as pd

# *Feature removed until CCP fixes market history API
# volumes = ['./market-data/volume data/market-history-2022-11-02.csv',
#             './market-data/volume data/market-history-2022-11-01.csv',
#             './market-data/volume data/market-history-2022-10-31.csv',
#             './market-data/volume data/market-history-2022-10-30.csv',
#             './market-data/volume data/market-history-2022-11-03.csv']
stations = ['./market-data/daily-data/domain_daily_data.csv',
            './market-data/daily-data/sinqlaison_daily_data.csv',
            './market-data/daily-data/metropolis_daily_data.csv',
            './market-data/daily-data/heimatar_daily_data.csv']
index_df = pd.read_csv('./helper-files/index_baskets.csv')
regions = ('./helper-files/region-to-system.csv')
jita_df = pd.read_csv('./market-data/daily-data/theforge_daily_data.csv')
jita_df = jita_df.groupby(['type_id'], as_index=False).max()
master_df = pd.DataFrame()
master_volume_df = pd.DataFrame()
final_df = pd.DataFrame()
blank_df = pd.DataFrame()
volume_df = pd.DataFrame()
master_volume_summary_df = pd.DataFrame()

# *Feature removed until CCP fixes market history API
###Pulls the volume history for the past # of days
# for volume in volumes:
#     print(volume)
#     volume_df = pd.read_csv(volume)
#     volume_df = volume_df.groupby(['region_id', 'type_id']).sum().reset_index()
#     master_volume_summary_df = pd.concat([master_volume_summary_df, volume_df], ignore_index=True, axis=0)
# region_id = pd.read_csv(regions)
# master_volume_summary_df = pd.merge(master_volume_summary_df, region_id, on=['region_id'], how='inner')
# master_volume_summary_df = master_volume_summary_df.groupby(['system_id', 'type_id']).sum().reset_index()
# master_volume_summary_df = master_volume_summary_df.drop(['average', 'highest', 'lowest', 'region_id'], axis=1)
# master_volume_summary_df = master_volume_summary_df.rename(columns={'volume' : 'volume_history'})
# master_volume_summary_df = master_volume_summary_df.rename(columns={'order_count' : 'order_count_history'})
for station in stations:
    print(station)
    station_df = pd.read_csv(station)
    station_df = station_df.groupby(['type_id'], as_index=False).min()
    master_df = pd.concat([master_df, station_df], ignore_index=True, axis=0)

for station in stations:
    print(station)
    station_volume_df = pd.read_csv(station)
    station_volume_df = station_volume_df.groupby(['system_id', 'type_id']).sum().reset_index()
    master_volume_df = pd.concat([master_volume_df, station_volume_df], ignore_index=True, axis=0)

master_volume_df = master_volume_df.drop(['is_buy_order', 'price'], axis=1)
# *Feature removed until CCP fixes market history API
# master_df = master_df.drop(['volume_remain'], axis=1)
# master_df = pd.merge(master_df, master_volume_df, on=['type_id', 'system_id'], how='inner')
# master_df = pd.merge(master_df, master_volume_summary_df, on=['system_id', 'type_id'], how='inner')
master_df = pd.merge(master_df, index_df, on='type_id', how='inner')
master_df = pd.merge(master_df, jita_df, on='type_id', how='inner')
master_df = master_df[master_df['groupID'].isin([1996, 376, 372, 374, 377, 1041, 4061, 656, 1989, 654, 655, 653, 375, 373, 645, 657, 1678, 1677, 1199, 1968, 1697, 1156, 1210, 98, 1774, 328, 329, 62, 1150, 1670, 96, 339, 367, 1042, 1890, 90, 863, 864, 303, 80, 842, 61, 76, 87, 768, 767, 43, 47, 1657, 330, 815, 100, 1770, 1027, 662, 484, 429, 1888, 4062, 334, 365, 958, 4127, 285, 386, 738, 1231, 739, 740, 741, 742, 744, 745, 746, 747, 743, 1229, 1230, 748, 749, 750, 1228, 751, 738, 300, 389, 838, 839, 60, 716, 538, 333, 728, 731, 730, 979, 729, 732, 734, 735, 733, 88, 954, 647, 645, 1292, 646, 313, 201, 514, 639, 326, 71, 544, 68, 1404, 1313, 1988, 4102, 1987, 765, 4072, 1063, 1026, 407, 1699, 1701, 1700, 1702, 649, 86, 483, 1136, 737, 1569, 59, 711, 1098, 205, 772, 1653, 385, 63, 85, 974, 977, 964, 465, 762, 1773, 4088, 4117, 548, 589, 428, 1229, 4041, 707, 590, 1652, 384, 640, 302, 2008, 530, 663, 1189, 1533, 18, 482, 101, 1771, 54, 1396, 1395, 1400, 862, 506, 512, 510, 771, 509, 1245, 511, 1679, 507, 508, 1674, 524, 1249, 4096, 1246, 1276, 4107, 438, 1275, 1250, 361, 4096, 427, 2018, 1964, 1676, 916, 763, 275, 1312, 953, 764, 493, 1033, 1032, 1035, 1031, 1875, 1889, 766, 1028, 289, 83, 957, 4086, 282, 769, 325, 67, 585, 290, 41, 209, 1308, 773, 781, 778, 786, 774, 776, 779, 782, 777, 1232, 1233, 774, 1234, 269, 387, 4142, 886, 1159, 754, 1122, 481, 709, 479, 1217, 1223, 1238, 340, 1206, 203, 212, 910, 208, 911, 440, 338, 40, 1769, 321, 38, 770, 77, 444, 57, 39, 295, 636, 1306, 48, 515, 210, 1154, 661, 436, 1739, 1772, 880, 993, 990, 992, 997, 991, 971, 72, 1112, 4091, 1672, 2013, 62, 464, 1240, 1311, 588, 1537, 492, 1226, 49, 4029, 379, 89, 473, 213, 909, 211, 907, 650, 1995, 2002, 462, 4060, 4067, 1289, 315, 899, 908, 52, 291, 1019, 476])]
master_df['buysell_difference'] = abs(master_df['price'] - master_df['jita_price'])
master_df['buysell_percentage'] = abs(master_df['jita_price'] - master_df['price']) / master_df['jita_price'] * 100

# *Feature removed until CCP fixes market history API
# The formula with volume history:
## master_df['formula'] = abs((master_df['buysell_percentage'] / master_df['volume_remain']) * master_df['volume_history'])
master_df['formula'] = abs((master_df['buysell_percentage'] / master_df['volume_remain']))
master_df = master_df[master_df.buysell_percentage < 2000]
master_df.to_csv("./market-data/daily-data/master_daily_data.csv", index=False)

station_ids = [30002187, 30002510, 30002659, 30002053]
for station in station_ids:
    if station == 30002187:
        amarr_df = master_df[master_df.system_id == 30002187]
        amarr_df = amarr_df.drop(['is_buy_order', 'primaryIndex', 'subIndex', 'type_id', 'groupName', 'categoryID', 'groupID'], axis=1)
        print('Amarr sorted by formula: ')
        print(amarr_df.sort_values('formula', ascending=False).head(100))
        final_amarr_df = (amarr_df.sort_values('formula', ascending=False).head(100))
    elif station == 30002510:
        rens_df = master_df[master_df.system_id == 30002510]
        rens_df = rens_df.drop(['is_buy_order', 'primaryIndex', 'subIndex', 'type_id', 'groupName', 'categoryID', 'groupID'], axis=1)
        print('Rens sorted by formula: ')
        print(rens_df.sort_values('formula', ascending=False).head(100))
        final_rens_df = (rens_df.sort_values('formula', ascending=False).head(100))
    elif station == 30002659:
        dodixie_df = master_df[master_df.system_id == 30002659]
        dodixie_df = dodixie_df.drop(['is_buy_order', 'primaryIndex', 'subIndex', 'type_id', 'groupName', 'categoryID', 'groupID'], axis=1)
        print('Dodixie sorted by formula: ')
        print(dodixie_df.sort_values('formula', ascending=False).head(100))
        final_dodixie_df = (dodixie_df.sort_values('formula', ascending=False).head(100))
    elif station == 30002053:
        hek_df = master_df[master_df.system_id == 30002053]
        hek_df = hek_df.drop(['is_buy_order', 'primaryIndex', 'subIndex', 'type_id', 'groupName', 'categoryID', 'groupID'], axis=1)
        print('Hek sorted by formula: ')
        print(hek_df.sort_values('formula', ascending=False).head(100))
        final_hek_df = (hek_df.sort_values('formula', ascending=False).head(100))
  

#Writes the desired outcomes to individual CSV's
final_amarr_df.to_csv("./market-data/final-info/final_amarr_data.csv", index=False)
final_dodixie_df.to_csv("./market-data/final-info/final_dodixie_data.csv", index=False)
final_rens_df.to_csv("./market-data/final-info/final_rens_data.csv", index=False)
final_hek_df.to_csv("./market-data/final-info/final_hek_data.csv", index=False)