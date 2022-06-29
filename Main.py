#bot.py
import os
from dotenv import load_dotenv
import discord
from discord.ext import commands
import asyncio
import random
from DBConnection import DataConnection

load_dotenv()
token = os.getenv('BOT_TOKEN')
bot = commands.Bot(command_prefix='&')
client = discord.Client()
loopEvt = asyncio.get_event_loop()

@bot.event
async def on_ready():
    
    await DataConnection.createTables(loopEvt)
    
    for guild in bot.guilds:

        await DataConnection.updateGuild(loopEvt, guild)
        await DataConnection.updateUser(loopEvt, guild.get_member(int(os.getenv('BOT_ID'))), guild)

        for channel in guild.voice_channels:

            await DataConnection.updateVChAndCt(loopEvt, channel, True, False, False)


@bot.event
async def on_guild_join(guild):

    await DataConnection.updateGuild(loopEvt, guild)
    await DataConnection.updateUser(loopEvt, guild.get_member(int(os.getenv('BOT_ID'))), guild)
    
    for channel in guild.voice_channels:
        await DataConnection.updateVChAndCt(loopEvt, channel, True, False, False)

    for channel in guild.text_channels:
        if channel.name == "bifrost":
            await channel.send("```fix I see you on the voice channels. But you are far away \n" + 
                                        "Nobody goes through the bifrost without me seeing it!```")


@bot.event
async def on_voice_state_update(member, before, after):

    await DataConnection.updateUser(loopEvt, member, member.guild)

    if after.self_mute and before.channel == after.channel:

        await DataConnection.voiceActivity(loopEvt, member, after.channel, True)

    elif not after.self_mute and before.channel == after.channel:

        await DataConnection.voiceActivity(loopEvt, member, after.channel, False)

    if before.channel == None and after.channel != None:
            
        await DataConnection.voiceSession(loopEvt, member, after.channel, True)

        if after.self_mute:

            await DataConnection.voiceActivity(loopEvt, member, after.channel, True)

    elif before.channel != None and after.channel != None and before.channel != after.channel:

        if before.self_mute:
            
            await DataConnection.voiceActivity(loopEvt, member, before.channel, False)

        await DataConnection.voiceSession(loopEvt, member, before.channel, False)
        await DataConnection.voiceSession(loopEvt, member, after.channel, True)

        if after.self_mute:

            await DataConnection.voiceActivity(loopEvt, member, after.channel, True)

    elif before.channel != None and after.channel == None:

        if before.self_mute:
            
            await DataConnection.voiceActivity(loopEvt, member, before.channel, False)
        
        await DataConnection.voiceSession(loopEvt, member, before.channel, False)


@bot.event
async def on_guild_channel_create(channel):

    if channel.type == discord.ChannelType.voice:

        await DataConnection.updateVChAndCt(loopEvt, channel, True, False, False)


@bot.event
async def on_guild_channel_update(before, after):

    if after.type == discord.ChannelType.voice:

        await DataConnection.updateVChAndCt(loopEvt, after, False, True, False)


@bot.event
async def on_guild_channel_delete(channel):

    if channel.type == discord.ChannelType.voice:

        await DataConnection.updateVChAndCt(loopEvt, channel, False, False, True)


@bot.event
async def on_user_update(before, after):
    await DataConnection.updateUser(loopEvt, after, after.guild)


@bot.event
async def on_guild_update(before, after):
    await DataConnection.updateGuild(loopEvt, after, False, True)


@bot.command(name='rank', help='Shows the voice participation rank.')
async def rank(ctx):

    rank = await DataConnection.rank(loopEvt, ctx)
    srt_formated = ''
    
    for user, time in rank.items():

        hour = time.total_seconds()/3600 + time.days *24
        minute = (hour % 1) * 60
        second = (minute % 1) * 60
        seperator = ''

        if hour > 0:

            if len("`{}° {}#{}••{}h::{}m::{}s`\n" .format(list(rank.keys()).index(user) + 1, 
                            user[1], user[2], int(hour), int(minute), int(second))) < 60:
                
                newSize = 24 - len(f'{user[1]}#{user[2]}')
                
                x = 0
                while x < newSize:
                    seperator += '─'
                    x += 1
                
                if ctx.author.id == user[0]:

                    srt_formated += "```HTTP\n{}° {}#{}•{}•{}h::{}m::{}s\n```\n" .format(list(rank.keys()).index(user) + 1, 
                                        user[1], user[2], seperator, int(hour), int(minute), int(second))

                else:

                    srt_formated += "```{}° {}#{}•{}•{}h::{}m::{}s```\n" .format(list(rank.keys()).index(user) + 1, 
                                        user[1], user[2], seperator, int(hour), int(minute), int(second))

    if srt_formated == '':

        srt_formated = "`Nothing`"

    embed = discord.Embed(
        title = "VPR - Voice Participation Rank",
        description = "Always showing the data of the last 7 days. Pay attention!",
        colour = discord.Colour.orange()
    )

    embed.add_field(name='TOP 10 talkative members', value=srt_formated, inline=False)

    await ctx.send(embed=embed)

async def formatChannelInfo(channel, time):

    srt_formated = ''
    hour = time.total_seconds()/3600 + time.days *24
    minute = (hour % 1) * 60
    second = (minute % 1) * 60
    seperator = ''
    if hour > 0 and second > 1:

        if len("```{}•{}•{}h::{}m::{}s```\n" .format(channel[1], seperator, int(hour), int(minute), int(second))) < 60:
            
            newSize = 24 - len(f'{channel[1]}')
            
            x = 0
            while x < newSize:
                seperator += '─'
                x += 1
            
            srt_formated += "```{}•{}•{}h::{}m::{}s```\n" .format(channel[1], seperator, int(hour), int(minute), int(second))
            
            return srt_formated
    
    else:

        return ""
        
@bot.command(name='profile', help='Shows all your voice profile')
async def profile(ctx):

    embed = discord.Embed(
        title = "VP - Voice Profile",
        description = f"Showing all your voice profile, **{ctx.author}**",
        colour = discord.Colour.blue()
    )

    profile = await DataConnection.profile(loopEvt, ctx)

    hour = profile[0].total_seconds()/3600 + profile[0].days *24
    minute = (hour % 1) * 60
    second = (minute % 1) * 60

    if hour > 0:

        embed.add_field(name='You spent around', value='```fix\n             {}h::{}m::{}s\n```' 
        .format(int(hour), int(minute), int(second)), inline=False)

    else: 
        
        embed.add_field(name='You spent around', value='```fix\n             {}h::{}m::{}s\n```' 
        .format(0, 0, 0), inline=False)

    for category, channels in profile[1].items():

        srt_formated = ''

        for channel, time in channels.items():

            if type(time) != dict:
            
                srt_formated += await formatChannelInfo(channel, time)
                
            else:

                srt_formated = "`Nothing`"

            if srt_formated == '':

                    srt_formated = "`Nothing`"

        embed.add_field(name=f'Category: {category[1]}', value=srt_formated, inline=False)

    srt_formated2 = ''

    if profile[2] != {}:

        for channel, time in profile[2].items():

            if type(time) != dict:
                
                srt_formated2 += await formatChannelInfo(channel, time)
    
    else:

        srt_formated2 = "`Nothing`"

    embed.add_field(name='No category', value=srt_formated2, inline=False)

    await ctx.send(embed=embed)

@bot.command(name='sync', help='Synchronizes the discord server with the database.')
async def rank(ctx):

    await DataConnection.updateGuild(loopEvt, ctx.guild)
    await DataConnection.updateUser(loopEvt, ctx.guild.get_member(int(os.getenv('BOT_ID'))), ctx.guild)
    
    for channel in ctx.guild.voice_channels:
        await DataConnection.updateVChAndCt(loopEvt, channel, True, False, False)

    for channel in ctx.guild.text_channels:
        if channel.name == "bifrost":
            await channel.send("```fix I see you on the voice channels. But you are far away \n" + 
                                        "Nobody goes through the bifrost without me seeing it!```")

bot.run(token)