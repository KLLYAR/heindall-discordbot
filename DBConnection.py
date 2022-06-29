import asyncio
import aiomysql
from datetime import datetime
from ConnectTo import ConnectTo as ct

class DataConnection:

    @staticmethod
    async def updateUser(loop, member, guild):

        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort,
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:
            try:
                
                await conn.begin()
                await cur.execute("SELECT id FROM user WHERE id = %(id)s", {'id' : member.id})

                if cur.rowcount < 1:

                    await cur.execute("REPLACE INTO user VALUES(%(id)s, %(name)s, %(discriminator)s, %(bot)s)",
                    {'id' : member.id, 'name' : member.name, 'discriminator' : member.discriminator, 'bot' : member.bot})

                await cur.execute("SELECT id FROM user_guild WHERE user_id = %(user_id)s and guild_id = %(guild_id)s",
                {'user_id' : member.id, 'guild_id' : guild.id})

                if cur.rowcount < 1:

                    await cur.execute("REPLACE INTO user_guild(user_id, guild_id, joined_at) VALUES(%(user_id)s, %(guild_id)s, %(joined_at)s)",
                    {'user_id' : member.id, 'guild_id' : guild.id, 'joined_at' : member.joined_at})
                
                await conn.commit()

            except Exception as e:
                await conn.rollback()
                print(e)
                

        conn.close()


    @staticmethod
    async def voiceSession(loop, member, channel, login):
        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort,
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:

            await cur.execute("SELECT outcoming FROM voice_session WHERE outcoming IS NULL and user_id = %(user_id)s and channel_id = %(channel_id)s",
            {'user_id' : member.id, 'channel_id' : channel.id})
            
            if cur.rowcount < 1 and login:

                await cur.execute("INSERT INTO voice_session (incoming, user_id, channel_id) VALUES(%(incoming)s, %(user_id)s, %(channel_id)s);",
                {'incoming' : datetime.utcnow(), 'user_id' : member.id, 'channel_id' : channel.id})

            else:

                await cur.execute("""UPDATE voice_session SET outcoming = %(outcoming)s WHERE outcoming IS NULL and user_id = %(user_id)s and channel_id = %(channel_id)s""",
                {'outcoming' : datetime.utcnow(), 'user_id' : member.id, 'channel_id' : channel.id})
            
            await conn.commit()

        conn.close()
         

    @staticmethod
    async def voiceActivity(loop, member, channel, mute):
        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort,
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:

            await cur.execute("SELECT id FROM voice_session WHERE outcoming IS NULL and user_id = %(user_id)s and channel_id = %(channel_id)s",
            {'user_id' : member.id, 'channel_id' : channel.id})
                
            voice_session_id = await cur.fetchall()

            if cur.rowcount == 1:

                await cur.execute("SELECT unmute FROM voice_activity WHERE unmute IS NULL and voice_session_id = %(voice_session_id)s",
                {'voice_session_id' : voice_session_id[0][0]})

                if cur.rowcount < 1 and mute:

                    await cur.execute("INSERT INTO voice_activity (mute, voice_session_id) VALUES(%(mute)s, %(voice_session_id)s);",
                    {'mute' : datetime.utcnow(), 'voice_session_id' : voice_session_id[0][0]})

                else:

                    await cur.execute("""UPDATE voice_activity SET unmute = %(unmute)s WHERE unmute IS NULL and voice_session_id = %(voice_session_id)s""",
                    {'unmute' : datetime.utcnow(), 'voice_session_id' : voice_session_id[0][0]})
            
            await conn.commit()

        conn.close()


    @staticmethod
    async def updateVChAndCt(loop, channel, create, update, delete):

        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort,
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:
            try:
                
                await conn.begin()
                if create:

                    await cur.execute("SELECT id FROM voice_channel WHERE id = %(id)s", {'id' : channel.id})
                    
                    if cur.rowcount < 1:

                        if channel.category != None:

                            await cur.execute("SELECT id FROM channel_category WHERE id = %(id)s", {'id' : channel.category.id})
                        
                            if cur.rowcount < 1:

                                await cur.execute("INSERT INTO channel_category (id, name, guild_id) VALUES(%(id)s, %(name)s, %(guild_id)s);", 
                                {'id' : channel.category.id, 'name' : channel.category.name, 'guild_id' : channel.category.guild.id})
                            
                            await cur.execute("INSERT INTO voice_channel (id, name, category_id, guild_id) VALUES(%(id)s, %(name)s, %(category_id)s ,%(guild_id)s);", 
                            {'id' : channel.id, 'name' : channel.name, 'category_id' : channel.category.id, 'guild_id' : channel.guild.id})
                        
                        else:

                            await cur.execute("INSERT INTO voice_channel (id, name, guild_id) VALUES(%(id)s, %(name)s,%(guild_id)s);", 
                            {'id' : channel.id, 'name' : channel.name, 'guild_id' : channel.guild.id})

                elif update:

                        if channel.category != None:

                            await cur.execute("UPDATE channel_category SET name = %(name)s WHERE id = %(id)s", 
                            {'name' : channel.category.name, 'id' : channel.category.id})

                            await cur.execute("UPDATE voice_channel SET name = %(name)s, category_id = %(category_id)s WHERE id = %(id)s", 
                            {'name' : channel.name, 'category_id' : channel.category.id, 'id' : channel.id})

                        else: 

                            await cur.execute("UPDATE voice_channel SET name = %(name)s, category_id = NULL WHERE id = %(id)s", 
                            {'name' : channel.name, 'id' : channel.id})

                elif delete:

                    await cur.execute("UPDATE voice_channel SET deleted = %(datenow)s WHERE id = %(id)s", 
                        {'datenow' : datetime.utcnow(), 'id' : channel.id})

                    # if channel.category != None:

                    #     await cur.execute("UPDATE channel_category SET deleted = %(datenow)s WHERE id = %(id)s", 
                    #         {'datenow' : datetime.utcnow(), 'id' : channel.category.id})
                
                await conn.commit()

            except Exception as e:
                await conn.rollback()
                print(e)

        conn.close()


    @staticmethod
    async def updateGuild(loop, guild):

        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort,
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:
            try:
                
                await conn.begin()

                await cur.execute("SELECT id FROM guild WHERE id = %(id)s", {'id' : guild.id})
                
                if cur.rowcount < 1:

                    await cur.execute("REPLACE INTO guild (id, name) VALUES(%(id)s, %(name)s);", 
                    {'id' : guild.id, 'name' : guild.name})
                
                await conn.commit()

            except Exception as e:
                await conn.rollback()
                print(e)

        conn.close()


    @staticmethod
    async def createTables(loop):

        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort,
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:
            try:
                await conn.begin()
                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.guild
                                        (id BIGINT, 
                                        name VARCHAR(255) NOT NULL,
                                        PRIMARY KEY (id));""" .format(ct.myDB))

                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.user
                                        (id BIGINT, 
                                        name VARCHAR(255) NOT NULL,
                                        discriminator INT NOT NULL,
                                        bot TINYINT,
                                        PRIMARY KEY (id));""" .format(ct.myDB))
                                        
                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.user_guild
                                        (id BIGINT AUTO_INCREMENT, 
                                        user_id BIGINT NOT NULL,
                                        guild_id BIGINT NOT NULL,
                                        joined_at DATETIME NOT NULL,
                                        PRIMARY KEY (id),
                                        FOREIGN KEY(user_id) REFERENCES user(id),
                                        FOREIGN KEY(guild_id) REFERENCES guild(id));""" .format(ct.myDB))  

                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.channel_category
                                        (id BIGINT,
                                        name VARCHAR(255) NOT NULL,
                                        deleted DATETIME,
                                        guild_id BIGINT NOT NULL,
                                        PRIMARY KEY (id),
                                        FOREIGN KEY(guild_id) REFERENCES guild(id));""" .format(ct.myDB))

                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.voice_channel
                                        (id BIGINT,
                                        name VARCHAR(255) NOT NULL,
                                        deleted DATETIME,
                                        category_id BIGINT,
                                        guild_id BIGINT NOT NULL,
                                        PRIMARY KEY (id),
                                        FOREIGN KEY(category_id) REFERENCES channel_category(id),
                                        FOREIGN KEY(guild_id) REFERENCES guild(id));""" .format(ct.myDB))

                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.voice_session
                                        (id BIGINT AUTO_INCREMENT,
                                        incoming DATETIME NOT NULL, 
                                        outcoming DATETIME DEFAULT NULL,
                                        user_id BIGINT NOT NULL,
                                        channel_id BIGINT NOT NULL,
                                        PRIMARY KEY (id),
                                        FOREIGN KEY (user_id) REFERENCES user(id),
                                        FOREIGN KEY (channel_id) REFERENCES voice_channel(id));""" .format(ct.myDB))
                
                await cur.execute("""CREATE TABLE IF NOT EXISTS {}.voice_activity
                                        (id BIGINT AUTO_INCREMENT,
                                        mute DATETIME NOT NULL, 
                                        unmute DATETIME DEFAULT NULL,
                                        voice_session_id BIGINT NOT NULL,
                                        PRIMARY KEY (id),
                                        FOREIGN KEY (voice_session_id) REFERENCES voice_session(id));""" .format(ct.myDB))
                
                await conn.commit()

            except Exception as e:
                await conn.rollback()
                print(e)

        conn.close()


    @staticmethod
    async def rank(loop, ctx):
        
        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort, 
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:

            aWeekBefore = datetime(datetime.utcnow().year, 
                        datetime.utcnow().month, 
                        datetime.utcnow().day - 7, 
                        datetime.utcnow().hour, 
                        datetime.utcnow().minute, 
                        datetime.utcnow().second)
            
            rank = {}
            reverse_rank = {}

            await cur.execute("SELECT * FROM user INNER JOIN user_guild ON user.id = user_guild.user_id WHERE bot = 0 and guild_id = %(guild_id)s LIMIT 10", {'guild_id' : ctx.guild.id})

            if cur.rowcount > 0:

                users = await cur.fetchall()

                for user in users:

                    rank[user] = aWeekBefore - aWeekBefore

                    await cur.execute("SELECT * FROM voice_session INNER JOIN voice_channel ON voice_session.channel_id = voice_channel.id WHERE incoming > %(aWeekBefore)s and user_id = %(user_id)s and guild_id = %(guild_id)s",
                    {'aWeekBefore' : aWeekBefore, 'user_id' : user[0], 'guild_id' : ctx.guild.id})

                    if cur.rowcount > 0:

                        voice_sessions = await cur.fetchall()

                        time = aWeekBefore - aWeekBefore

                        for vSession in voice_sessions:

                            if vSession[2] == None:

                                time += (datetime.utcnow() - vSession[1])

                            else:

                                time += (vSession[2] - vSession[1])

                            await cur.execute("SELECT id, mute, unmute, voice_session_id FROM voice_activity WHERE mute IS NOT NULL and voice_session_id = %(voice_session_id)s", 
                            {'voice_session_id' : vSession[0]})

                            if cur.rowcount > 0:

                                voice_activities = await cur.fetchall()
                                
                                for vActivity in voice_activities:

                                    if vSession[2] == None:

                                        time -= (datetime.utcnow() - vActivity[1])

                                    else:
                                        
                                        time -= (vActivity[2] - vActivity[1])
            
                        rank[user] = time

                sorted_rank = {key: value for key, value in sorted(rank.items(), key=lambda item: item[1])}

                for k,i in sorted_rank.items():

                    dict_element = {k:i}
                    dict_element.update(reverse_rank)
                    reverse_rank = dict_element
                
            return reverse_rank
        conn.close()


    @staticmethod
    async def profile(loop, ctx):
        conn = await aiomysql.connect(host=ct.myHost, port=ct.myPort, 
                                        user=ct.myUser, password=ct.myPassWd, db=ct.myDB, loop=loop)
        cur = await conn.cursor()
        async with conn.cursor() as cur:

            categorysD = {}
            reverse_category = {}

            noCategory = {}
            reverse_noCategory = {}

            wholeTime = datetime.utcnow() - datetime.utcnow()

            await cur.execute("SELECT * FROM channel_category WHERE guild_id = %(guild_id)s", {'guild_id' : ctx.guild.id})

            if cur.rowcount > 0:

                categorys = await cur.fetchall()

                for category in categorys:

                    categorysD[category] = {}

                    await cur.execute("SELECT * FROM voice_channel WHERE guild_id = %(guild_id)s", 
                    {'guild_id' : ctx.guild.id})
                    
                    if cur.rowcount > 0:

                        channels = await cur.fetchall()

                        for channel in channels:

                            time = datetime.utcnow() - datetime.utcnow()

                            await cur.execute("SELECT * FROM voice_session WHERE user_id = %(user_id)s and channel_id = %(channel_id)s",
                            {'user_id' : ctx.author.id, 'channel_id' : channel[0]})
                            
                            if cur.rowcount > 0:

                                voice_sessions = await cur.fetchall()

                                for vSession in voice_sessions:

                                    if vSession[2] == None:

                                        time += (datetime.utcnow() - vSession[1])
                                        
                                    else:

                                        time += (vSession[2] - vSession[1])

                                    await cur.execute("SELECT id, mute, unmute, voice_session_id FROM voice_activity WHERE mute IS NOT NULL and voice_session_id = %(voice_session_id)s", 
                                    {'voice_session_id' : vSession[0]})

                                    if cur.rowcount > 0:

                                        voice_activities = await cur.fetchall()
                                        
                                        for vActivity in voice_activities:

                                            if vSession[2] == None:

                                                time -= (datetime.utcnow() - vActivity[1])

                                            else:
                                                
                                                time -= (vActivity[2] - vActivity[1])
                            
                            wholeTime += time

                            if channel[3] == category[0]:

                                categorysD[category][channel] = time

                            if channel[3] == None:
                                
                                noCategory[channel] = time

                        sorted_noCategory = {key: value for key, value in sorted(noCategory.items(), key=lambda item: item[1])}

                        for k,i in sorted_noCategory.items():

                            dict_element = {k:i}
                            dict_element.update(reverse_noCategory)
                            reverse_noCategory = dict_element

                sorted_category = {key: value for key, value in sorted(categorysD[category].items(), key=lambda item: item[1])}

                for k, i in sorted_category.items():
                    dict_element = {k:i}
                    dict_element.update(reverse_category)
                    reverse_category = dict_element
        
                categorysD[category] = reverse_category
                
                reverse_profile = [wholeTime, categorysD, reverse_noCategory]

                return reverse_profile

        conn.close()