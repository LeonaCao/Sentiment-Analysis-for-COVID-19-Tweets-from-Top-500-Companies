import json
import gzip
import binascii
import os
import pandas as pd
import pymysql
from datetime import datetime

conn = pymysql.connect(host='host',
                       port=port,
                       user='user',
                       passwd='password',
                       database='database',
                       charset='charset')
cursor = conn.cursor()

# Specify the directory with scraped tweets files
base_dir = '~/file folder'

# List the top 500 companies
companies_list = ['Microsoft','Apple', 'facebook', 'jpmorgan', 'Google', 'JNJCares', 'Visa', 'ProcterGamble', 'exxonmobil', 'ATT', 'BankofAmerica',
                  'WaltDisneyCo', 'UnitedHealthGrp', 'intel', 'Mastercard', 'verizon', 'HomeDepot', 'Chevron', 'Merck', 'WellsFargo', 'pfizer',
                  'CocaCola', 'comcast', 'Boeing', 'Cisco', 'pepsi', 'Walmart', 'Citi', 'Medtronic', 'AbbottGlobal', 'McDonalds', 'Adobe',
                  'salesforce', 'Amgen', 'netflix', 'bmsnews', 'Costco', 'InsidePMI', 'nvidia', 'Accenture', 'thermofisher', 'honeywell', 'PayPal',
                  'Broadcom', 'UnionPacific', 'Oracle', 'Nike', 'IBM', 'nexteraenergy', 'TXInstruments', 'Lindeplc', 'LillyPad', 'Starbucks',
                  'CVSHealth', 'LockheedMartin', '3M', 'Qualcomm', 'DanaherCareers', 'generalelectric', 'AltriaNews', 'Lowes', 'usbank', 'FISGlobal',
                  'GileadSciences', 'BookingHoldings', 'AmericanExpress', 'UPS', 'CaterpillarInc', 'MDLZ', 'GetSpectrum', 'Cigna', 'ADP', 'CMEGroup',
                  'BBT', 'TJXCareers', 'AnthemInc', 'GoldmanSachs', 'Chubb', 'BDandCo', 'conocophillips', 'PNCBank', 'SPGlobal', 'Intuit',
                  'IntuitiveSurg', 'DominionEnergy', 'DukeEnergy', 'Fiserv', 'Target', 'Stryker_Jobs', 'SouthernCompany', 'MorganStanley', 'bostonsci',
                  'Allergan', 'Raytheon', 'Zoetis', 'CP_News', 'blackrock', 'Prologis', 'CharlesSchwab', 'CSX', 'VertexPharma', 'MicronTech',
                  'MMC_Global', 'Applied4Tech', 'JohnDeere', 'CrownCastle', 'northropgrumman', 'biogen', 'GlobalPayInc', 'Phillips66Co', 'airproducts',
                  'ICE_Markets', 'biogen', 'nscorp', 'Equinix', 'Aon_plc', 'Humana', 'EdwardsLifesci', 'CapitalOne', 'illumina', 'Ecolab',
                  'L3HarrisTech', 'Emerson_News', 'KCCorp', 'AEPnews', 'SherwinWilliams', 'EsteeLauder', 'DuPont_News', 'ATVI_AB', 'WasteManagement',
                  'GM', 'SimonPropertyGp', 'Walgreens', 'AIGinsurance', 'Progressive', 'Exelon', 'baxter_intl', 'Sysco', 'Ross_Stores', 'BNYMellon',
                  'LamResearch', 'generaldynamics', 'SempraEnergy', 'Kinder_Morgan', 'autodesk', 'WeAreOxy', 'MetLife', 'MarriottIntl', 'HCAhealthcare',
                  'eatoncorp', 'MoodysInvSvc', 'DollarGeneral', 'DowNewsroom', 'aflac', 'ValeroEnergy', 'MarathonPetroCo', 'Prudential', 'FedEx',
                  'Allstate', 'Ford', 'Travelers', 'Delta', 'Cognizant', 'oreillyauto', 'TEConnectivity', 'xcelenergy', 'cbrands', 'AmphenolPCD',
                  'EA', 'PublicStorage', 'johnsoncontrols', 'GeneralMills', 'HP', 'IngersollRand', 'VFCorp', 'IHSMarkit', 'yumbrands', 'ONEOK',
                  'HiltonHotels', 'Regeneron', 'zimmerbiomet', 'PPG', 'StateStreet', 'TRowePrice', 'PSEGNews', 'ConEdison', 'AvalonBay',
                  'IQVIA_global', 'MotoSolutions', 'EquityRes', 'WilliamsUpdates', 'KLAcorp', 'Paychex', 'Agilent', 'TysonFoods', 'EversourceCorp',
                  'autozone', 'paccar', 'MicrochipTech', 'eBay', 'Cummins', 'Discover', 'WTWcorporate', 'Centene', 'McKesson', 'firstenergycorp',
                  'SouthwestAir', 'PPLCorp', 'Verisk', 'Twitter', 'MonsterEnergy', 'XilinxInc', 'AlexionPharma', 'StanleyBlkDeckr', 'TMobile',
                  'DTE_Energy', 'digitalrealty', 'RealtyIncome', 'ADMupdates', 'IDEXX', 'LasVegasSands', 'Entergy', 'PXDtweets', 'Cerner',
                  'ROKAutomation', 'CintasCorp', 'LyondellBasell', 'Corning', 'FortiveCorp', 'NorthernTrust', 'Aptiv', 'MSCI_Inc', 'Weyerhaeuser',
                  'amwater', 'BallCorpHQ', 'ResMed', 'RoyalCaribbean', 'ANSYS', 'AMETEKInc', 'kroger', 'VERISIGN', 'synopsys', 'HersheyCompany',
                  'ChipotleTweets', 'TheHartford', 'ameriprise', 'MandT_Bank', 'DollarTree', 'synchrony', 'ViacomCBS', 'corteva', 'Halliburton',
                  'FastenalCompany', 'BestBuy', 'McCormickCorp', 'FifthThird', 'CBRE', 'mettlertoledo', 'EssexProperties', 'Copart', 'bxpboston',
                  'westerndigital', 'CarnivalPLC', 'CloroxCo', 'CDWCorp', 'firstrepublic', 'Keysight', 'keybank', 'HP', 'AmerenCorp', 'DRHorton',
                  'KraftHeinzCo', 'ConsumersEnergy', 'Lennar', 'Equifax', 'united', 'GallagherGlobal', 'KelloggCompany', 'IntlPaperCo', 'CitizensBank',
                  'maximintegrated', 'amcorpackaging', 'DoverCorp', 'Omnicom', 'Fortinet', 'HessCorporation', 'MGMResortsIntl', 'CarMax',
                  'ConagraBrands', 'cardinalhealth', 'symantec', 'Paycom', 'ultabeauty', 'Akamai', 'citrix', 'WatersCorp', 'ExpediaGroup',
                  'askRegions', 'Broadridge', 'QuestDX', 'Garmin', 'Hologic', 'XylemInc', 'IFF', 'TiffanyAndCo', 'Gartner_inc', 'Seagate', 'darden',
                  'grainger', 'bakerhughesco', 'genuinepartsco', 'WabtecCorp', 'principal', 'UDRMarketing', 'Huntington_Bank', 'atmosenergy',
                  'alliantenergy', 'extraspace', 'MASCOLMA', 'Loews_Hotels', 'Healthcare_ABC', 'CenturyLink', 'CBOE', 'TheAESCorp', 'SVB_Financial',
                  'FOXTV', 'DukeRealty', 'Nasdaq', 'Incyte', 'Hasbro', 'ZebraTechnology', 'RyanHomes1948, NVHomes1979', 'energyinsights', 'NetApp',
                  'celanese', 'STERIS', 'MarketAxess', 'JacobsConnects', 'VarianMedSys', 'DentsplySirona', 'QorvoInc', 'HormelFoods', 'JackHenryAssoc',
                  'AllegionPlc', 'Seagate', 'AristaNetworks', 'brownforman', 'EXPD_Official', 'smuckers', 'arconic', 'WesternUnion', 'PulteHomes',
                  'lincolnfingroup', 'AmericanAir', 'EverestIns', 'TractorSupply', 'UnitedRentals', 'AveryDennison', 'UHS_Inc', 'MylanNews',
                  'WRBerkleyCorp', 'Textron', 'CruiseNorwegian', 'HIIndustries', 'NiSourceInc', 'MolsonCoors', 'WestRock', 'vornado', 'PerkinElmer',
                  'ApacheCorp', 'HenrySchein', 'regencycenters', 'IronMountain', 'CHRobinson', 'AdvanceAuto', 'AlbemarleCorp', 'CampbellSoupCo',
                  'GlobeLife', 'nrgenergy', 'packagingcorp', 'LKQCorp', 'FederalRealty', 'FBHS_News', 'DiscoveryIncTV', 'CFIndustries', 'jbhunt360',
                  'EastmanChemCo', 'Snapon_Tools', 'InterpublicIPG', 'WhirlpoolCorp', 'LiveNation', 'dish', 'DaVita', 'AimcoApts', 'Assurant',
                  'JuniperNetworks', 'PerrigoCompany', 'kimcorealty', 'F5Networks', 'abiomedimpella', 'CabotOG', 'SLGreen', 'ComericaBank', 'Pentair',
                  'nblenergy', 'PeoplesUnited', 'FTI_Global', 'ZionsBank', 'tapestryInc', 'NOVGlobal', 'Xerox', 'BorgWarner', 'AOSmithHotWater',
                  'CruiseNorwegian', 'RollinsPC', 'AlaskaAir', 'newell_brands', 'roberthalf', 'Kohls', 'nielsen', 'flir', 'Quanta_Services',
                  'RalphLauren', 'L_Brands', 'PVHCorp', 'InvescoUS', 'Sealed_Air', 'MosaicCompany', 'TechnipFMC', 'Flowserve', 'unumnews',
                  'MarathonOil', 'DevonEnergy', 'Macys', 'Nordstrom', 'GapInc', 'AllianceData', 'COTYInc', 'HelmerichPayne', 'UnderArmour']


# Get all files from the directory
data_list = []
for file in os.listdir(base_dir):

    # If file is a json, construct its full path and open it, append all json data to list
    if 'jsonl' in file:
        json_path = os.path.join(base_dir, file)
        print(json_path)

        filename = json_path  # Sample file

        json_content = []
        try:
            with gzip.open(filename, 'rb') as gzip_file:
                for line in gzip_file:  # Read one line
                    line = line.rstrip()
                    if line:  # Any JSON data on it?
                        obj = json.loads(line)
                        json_content.append(obj)


            # print(json.dumps(json_content, indent=4)) # Pretty-print data parsed.
            # items = json.dumps(json_content, indent=4)
            # print(len(items))

            def inputdict4sql(item, item_name):
                try:
                    tmpitem = item[item_name]
                except:
                    tmpitem = 'null'
                return tmpitem


            for item in json_content:
                language = inputdict4sql(item, 'lang')
                in_reply_to_screen_name = inputdict4sql(item, 'in_reply_to_screen_name')

                if (language == 'en') & (in_reply_to_screen_name in companies_list):
                    print(1)
                    try:
                        screen_name = inputdict4sql(item['user'], 'screen_name')
                        created_at = datetime.strptime(inputdict4sql(item, 'created_at'),
                                                       '%a %b %d %H:%M:%S +0000 %Y')
                        tweet_id = inputdict4sql(item, 'id')
                        text = inputdict4sql(item, 'full_text')
                        retweet_count = inputdict4sql(item, 'retweet_count')
                        favorite_count = inputdict4sql(item, 'favorite_count')

                        in_reply_to_screen_name = inputdict4sql(item, 'in_reply_to_screen_name')
                        in_reply_to_status_id = inputdict4sql(item, 'in_reply_to_status_id')
                        in_reply_to_user_id = inputdict4sql(item, 'in_reply_to_user_id')
                        geo = inputdict4sql(item, 'geo')
                        coordinates = inputdict4sql(item, 'coordinates')
                        place = inputdict4sql(item, 'place')

                        # continue
                        t = [screen_name, created_at, tweet_id, text, retweet_count, favorite_count,
                             in_reply_to_screen_name,
                             in_reply_to_status_id, in_reply_to_user_id, geo, coordinates, place, language]
                        sql = "insert into covid_4(screen_name,created_at,tweet_id,text,retweet_count,favorite_count,in_reply_to_screen_name,in_reply_to_status_id, in_reply_to_user_id,geo,coordinates,place,language) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        print('sql')
                        cursor.execute(sql, t)
                        print('t')
                        conn.commit()
                    except:
                        print(item)
                        with open('except.json', 'a') as outfile:
                            json.dump(item, outfile)
                continue

        except:
            pass