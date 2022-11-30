import pandas as pd
from urllib.error import HTTPError

### About 97 pages in about 1 minutes
domain = 'https://esi.evetech.net/latest/markets/10000043/orders/?datasource=tranquility&order_type=sell&page=1'
master_domain_df = pd.read_json(domain)
x_domain = 2

while True:
    try:
        domain2 = 'https://esi.evetech.net/latest/markets/10000043/orders/?datasource=tranquility&order_type=sell&page=' + str(x_domain)
        domain_df = pd.read_json(domain2)
        master_domain_df = pd.concat([master_domain_df, domain_df], ignore_index=True, axis=0)
        x_domain += 1
        print('Domain page: ' + str(x_domain) + ' complete')
    except HTTPError as err:
        if err.code == 404:
            break
###pulls just Amarr, Drops useless columns, writes to CSV
master_domain_df = master_domain_df[master_domain_df.system_id == 30002187]
master_domain_df = master_domain_df.drop(['duration', 'issued', 'min_volume', 'order_id', 'range', 'volume_total', 'location_id'], axis=1)
master_domain_df.to_csv("./market-data/domain_daily_data.csv", index=False)

### About 302 pages in about 5 minutes
theforge = 'https://esi.evetech.net/latest/markets/10000002/orders/?datasource=tranquility&order_type=buy&page=1'
master_theforge_df = pd.read_json(theforge)
x_theforge = 2

while True:
    try:
        theforge2 = 'https://esi.evetech.net/latest/markets/10000002/orders/?datasource=tranquility&order_type=buy&page=' + str(x_theforge)
        theforge_df = pd.read_json(theforge2)
        master_theforge_df = pd.concat([master_theforge_df, theforge_df], ignore_index=True, axis=0)
        x_theforge += 1
        print('The forge page: ' + str(x_theforge) + ' complete')
    except HTTPError as err:
        if err.code == 404:
            break
###pulls just Jita, Drops useless columns, writes to CSV
master_theforge_df = master_theforge_df[master_theforge_df.system_id == 30000142]
master_theforge_df = master_theforge_df.drop(['duration', 'issued', 'min_volume', 'order_id', 'range', 'volume_total', 'location_id', 'is_buy_order', 'system_id', 'volume_remain'], axis=1)
master_theforge_df.rename(columns={'price' : 'jita_price'}, inplace=True)
master_theforge_df.to_csv("./market-data/theforge_daily_data.csv", index=False)

### About 96 pages in about 2 minutes
sinqlaison = 'https://esi.evetech.net/latest/markets/10000032/orders/?datasource=tranquility&order_type=sell&page=1'
master_sinqlaison_df = pd.read_json(sinqlaison)
x_sinqlaison = 2

while True:
    try:
        sinqlaison2 = 'https://esi.evetech.net/latest/markets/10000032/orders/?datasource=tranquility&order_type=sell&page=' + str(x_sinqlaison)
        sinqlaison_df = pd.read_json(sinqlaison2)
        master_sinqlaison_df = pd.concat([master_sinqlaison_df, sinqlaison_df], ignore_index=True, axis=0)
        x_sinqlaison += 1
        print('Sinq Laison page: ' + str(x_sinqlaison) + ' complete')
    except HTTPError as err:
        if err.code == 404:
            break
###pulls just Rens, Drops useless columns, writes to CSV
master_sinqlaison_df = master_sinqlaison_df[master_sinqlaison_df.system_id == 30002659]
master_sinqlaison_df = master_sinqlaison_df.drop(['duration', 'issued', 'min_volume', 'order_id', 'range', 'volume_total', 'location_id'], axis=1)
master_sinqlaison_df.to_csv("./market-data/sinqlaison_daily_data.csv", index=False)

### About 96 pages in about 2 minutes
metropolis = 'https://esi.evetech.net/latest/markets/10000042/orders/?datasource=tranquility&order_type=sell&page=1'
master_metropolis_df = pd.read_json(metropolis)
x_metropolis = 2

while True:
    try:
        metropolis2 = 'https://esi.evetech.net/latest/markets/10000042/orders/?datasource=tranquility&order_type=sell&page=' + str(x_metropolis)
        metropolis_df = pd.read_json(metropolis2)
        master_metropolis_df = pd.concat([master_metropolis_df, metropolis_df], ignore_index=True, axis=0)
        x_metropolis += 1
        print('Metropolis page: ' + str(x_metropolis) + ' complete')
    except HTTPError as err:
        if err.code == 404:
            break
###pulls just Rens, Drops useless columns, writes to CSV
master_metropolis_df = master_metropolis_df[master_metropolis_df.system_id == 30002053]
master_metropolis_df = master_metropolis_df.drop(['duration', 'issued', 'min_volume', 'order_id', 'range', 'volume_total', 'location_id'], axis=1)
master_metropolis_df.to_csv("./market-data/metropolis_daily_data.csv", index=False)

### About 65 pages in about 2 minutes
heimatar = 'https://esi.evetech.net/latest/markets/10000030/orders/?datasource=tranquility&order_type=sell&page=1'
master_heimatar_df = pd.read_json(heimatar)
x_heimatar = 2

while True:
    try:
        heimatar2 = 'https://esi.evetech.net/latest/markets/10000030/orders/?datasource=tranquility&order_type=sell&page=' + str(x_heimatar)
        heimatar_df = pd.read_json(heimatar2)
        master_heimatar_df = pd.concat([master_heimatar_df, heimatar_df], ignore_index=True, axis=0)
        x_heimatar += 1
        print('Heimatar page: ' + str(x_heimatar) + ' complete')
    except HTTPError as err:
        if err.code == 404:
            break
###pulls just Rens, Drops useless columns, writes to CSV
master_heimatar_df = master_heimatar_df[master_heimatar_df.system_id == 30002510]
master_heimatar_df = master_heimatar_df.drop(['duration', 'issued', 'min_volume', 'order_id', 'range', 'volume_total', 'location_id'], axis=1)
master_heimatar_df.to_csv("./market-data/heimatar_daily_data.csv", index=False)
