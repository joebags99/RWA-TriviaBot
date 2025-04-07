import os
import pandas as pd
import discord
from discord.ext import commands, tasks
from datetime import datetime, timedelta
import asyncio
import requests
import json
import pymysql
import pymysql.cursors
from discord.ui import Button, View
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging - fixed log file name to be consistent
LOG_FILE = "trivia_bot.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("trivia_bot")

# ------------------------------------------------------------
# 1) CONFIGURATION - SET THESE VALUES FOR YOUR SETUP
# ------------------------------------------------------------
TOKEN = os.getenv("DISCORD_BOT_TOKEN")  # Environment variable for bot token
GUILD_ID = 747249327671476275  # Replace with the numeric ID of your server

# Role names (must match what you have in your Discord server)
WEEKLY_CHAMPION_ROLE = "Last Week's Champion"
ALL_TIME_CHAMPION_ROLE = "All Time Champion"

# Channel IDs for notifications
NOTIFICATION_CHANNEL_ID = int(os.getenv("NOTIFICATION_CHANNEL_ID", "0"))  # Replace with your channel ID
REACTION_ROLE_CHANNEL_ID = int(os.getenv("REACTION_ROLE_CHANNEL_ID", "0"))  # Replace with your channel ID

# File backup for scores (in case DB is down)
SCORES_FILE = r"/home/ec2-user/RWA-TriviaBot/trivia_scores.csv"

# MySQL Database Configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Reaction Role Configuration
REACTION_ROLES = {
    "🎮": "Twitch Viewer",
    "📺": "Youtube Viewer",
    "🎭": "Crimson Court Updates",
    "🌟": "All RWA Updates"
}

# Bot description
BOT_DESCRIPTION = "Royal Scribe - The official bot for Roll With Advantage"

# ------------------------------------------------------------
# 2) DISCORD INTENTS AND BOT SETUP
# ------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.reactions = True  # Need this for reaction roles

bot = commands.Bot(command_prefix="?", intents=intents, description=BOT_DESCRIPTION, help_command=None)

# Database connection cache
db_connection = None

# ------------------------------------------------------------
# 3) DATABASE HELPER FUNCTIONS
# ------------------------------------------------------------
def get_db_connection():
    """Creates a connection to the MySQL database with connection pooling."""
    global db_connection
    
    try:
        # Check if existing connection is valid
        if db_connection is not None:
            try:
                db_connection.ping(reconnect=True)
                return db_connection
            except:
                # If ping fails, connection is dead, so we'll create a new one
                pass
        
        # Create new connection
        db_connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,  # We'll manage transactions manually
        )
        return db_connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def execute_db_query(query, params=None, fetch=True, commit=True):
    """Execute a database query with proper error handling and connection management."""
    connection = get_db_connection()
    if not connection:
        logger.error("Failed to connect to database")
        return None
    
    result = None
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
            
        if commit:
            connection.commit()
            
        return result
    except Exception as e:
        logger.error(f"Database query error: {e}")
        if commit:
            try:
                connection.rollback()
            except:
                pass
        return None

def create_tables_if_not_exist():
    """Creates the necessary database tables if they don't exist."""
    # Create user mapping table for Twitch to Discord
    user_mapping_query = """
    CREATE TABLE IF NOT EXISTS user_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        twitch_username VARCHAR(255) NOT NULL,
        discord_id VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY twitch_username (twitch_username)
    )
    """
    
    # Create reaction roles table
    reaction_roles_query = """
    CREATE TABLE IF NOT EXISTS reaction_roles (
        id INT AUTO_INCREMENT PRIMARY KEY,
        message_id VARCHAR(255) NOT NULL,
        emoji VARCHAR(255) NOT NULL,
        role_id VARCHAR(255) NOT NULL,
        UNIQUE KEY message_emoji (message_id, emoji)
    )
    """
    
    if execute_db_query(user_mapping_query, fetch=False) is not None and \
       execute_db_query(reaction_roles_query, fetch=False) is not None:
        logger.info("Database tables created successfully")
        return True
    
    logger.error("Failed to create database tables")
    return False

def get_scores_from_external_db():
    """Gets scores from the external database."""
    query = """
    SELECT 
        user_id as id, 
        score AS Score, 
        lastUpdated AS RecentDate,
        createdAt AS FirstDate,
        username AS Username
    FROM user_scores
    WHERE username IS NOT NULL
    """
    
    results = execute_db_query(query)
    if not results:
        # Attempt to read from local CSV as fallback
        try:
            logger.info("Database unavailable, attempting to read from local CSV")
            if os.path.exists(SCORES_FILE):
                df = pd.read_csv(SCORES_FILE)
                return df
        except Exception as e:
            logger.error(f"Error reading local CSV file: {e}")
        return pd.DataFrame()
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Backup to CSV
    try:
        df.to_csv(SCORES_FILE, index=False)
        logger.info(f"Backed up scores to {SCORES_FILE}")
    except Exception as e:
        logger.warning(f"Failed to backup scores to CSV: {e}")
    
    return df

def map_twitch_to_discord(twitch_username, discord_id):
    """Maps a Twitch username to a Discord ID."""
    query = """
    INSERT INTO user_mapping (twitch_username, discord_id) VALUES (%s, %s) 
    ON DUPLICATE KEY UPDATE discord_id = %s
    """
    return execute_db_query(query, (twitch_username, discord_id, discord_id), fetch=False) is not None

def get_discord_id_from_twitch(twitch_username):
    """Gets the Discord ID mapped to a Twitch username."""
    query = "SELECT discord_id FROM user_mapping WHERE twitch_username = %s"
    result = execute_db_query(query, (twitch_username,))
    
    if result and len(result) > 0:
        return result[0]['discord_id']
    return None

def get_twitch_from_discord_id(discord_id):
    """Gets the Twitch username mapped to a Discord ID."""
    query = "SELECT twitch_username FROM user_mapping WHERE discord_id = %s"
    result = execute_db_query(query, (discord_id,))
    
    if result and len(result) > 0:
        return result[0]['twitch_username']
    return None

def export_mappings_to_csv():
    """Exports all user mappings to a CSV file."""
    query = "SELECT twitch_username, discord_id, created_at FROM user_mapping"
    results = execute_db_query(query)
    
    if not results or len(results) == 0:
        return False, "No mappings found to export"
    
    try:
        # Convert to DataFrame and save to CSV
        df = pd.DataFrame(results)
        export_file = "user_mappings_export.csv"
        df.to_csv(export_file, index=False)
        
        return True, export_file
    except Exception as e:
        logger.error(f"Error exporting mappings: {e}")
        return False, str(e)

def import_mappings_from_csv(file_path):
    """Imports user mappings from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        if 'twitch_username' not in df.columns or 'discord_id' not in df.columns:
            return False, "CSV file does not have required columns (twitch_username, discord_id)"
        
        success_count = 0
        error_count = 0
        
        connection = get_db_connection()
        if not connection:
            return False, "Database connection failed"
        
        try:
            with connection.cursor() as cursor:
                for _, row in df.iterrows():
                    try:
                        cursor.execute(
                            "INSERT INTO user_mapping (twitch_username, discord_id) VALUES (%s, %s) "
                            "ON DUPLICATE KEY UPDATE discord_id = %s",
                            (row['twitch_username'], row['discord_id'], row['discord_id'])
                        )
                        success_count += 1
                    except Exception:
                        error_count += 1
                        
            connection.commit()
            return True, f"Imported {success_count} mappings successfully. {error_count} errors."
        except Exception as e:
            logger.error(f"Error during import: {e}")
            connection.rollback()
            return False, str(e)
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return False, str(e)

def save_reaction_role(message_id, emoji, role_id):
    """Saves a reaction role mapping to the database."""
    query = """
    INSERT INTO reaction_roles (message_id, emoji, role_id) VALUES (%s, %s, %s) 
    ON DUPLICATE KEY UPDATE role_id = %s
    """
    return execute_db_query(query, (str(message_id), str(emoji), str(role_id), str(role_id)), fetch=False) is not None

def get_reaction_roles():
    """Gets all reaction role mappings from the database."""
    query = "SELECT message_id, emoji, role_id FROM reaction_roles"
    results = execute_db_query(query)
    
    if not results:
        return {}
    
    # Structure as a nested dictionary for easy lookup
    reaction_roles = {}
    for row in results:
        message_id = row["message_id"]
        emoji = row["emoji"]
        role_id = row["role_id"]
        
        if message_id not in reaction_roles:
            reaction_roles[message_id] = {}
        
        reaction_roles[message_id][emoji] = role_id
    
    return reaction_roles

# ------------------------------------------------------------
# 5) ON_READY EVENT (STARTS SCHEDULED TASKS)
# ------------------------------------------------------------
@bot.event
async def on_ready():
    # General info
    logger.info(f"✅ {bot.user} is now running!")
    logger.info(f"🌐 Connected to Discord as: {bot.user.name} (ID: {bot.user.id})")

    # Update bot's status with custom activity
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="Roll With Advantage"
        )
    )

    # Check your main guild
    guild = bot.get_guild(GUILD_ID)
    if guild:
        logger.info(f"Guild: {guild.name} has {len(guild.members)} members from the bot's view.")
    else:
        logger.error("❌ Guild not found with GUILD_ID =", GUILD_ID)

    # Create database tables if they don't exist
    create_tables_if_not_exist()

    # Start your scheduled tasks if not already running
    if not schedule_weekly_update.is_running():
        schedule_weekly_update.start()

# ------------------------------------------------------------
# 6) LEADERBOARD AND USER COMMANDS
# ------------------------------------------------------------
@bot.command()
async def leaderboard(ctx):
    """Displays the current leaderboard."""
    scores_df = get_scores_from_external_db()
    if scores_df.empty:
        await ctx.send("No scores available!")
        return
    
    # Find the most recent date in the database
    most_recent_date = scores_df['RecentDate'].max()
    
    # Filter scores from the most recent date
    recent_scores = scores_df[scores_df['RecentDate'] == most_recent_date].sort_values(by='Score', ascending=False)
    
    # Create embed for leaderboard
    embed = discord.Embed(
        title="📜 Current Leaderboard",
        description=f"Top scores as of {most_recent_date}",
        color=discord.Color.gold()
    )
    
    # Add top 10 scores
    for i, (_, row) in enumerate(recent_scores.head(10).iterrows()):
        username = row['Username'] if row['Username'] else "Unknown"
        score = row['Score']
        
        # Try to get Discord user if mapped
        discord_id = get_discord_id_from_twitch(username)
        if discord_id:
            embed.add_field(
                name=f"{i+1}. {username}",
                value=f"<@{discord_id}> • {score} points",
                inline=False
            )
        else:
            embed.add_field(
                name=f"{i+1}. {username}",
                value=f"{score} points",
                inline=False
            )
    
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    await ctx.send(embed=embed)

@bot.command()
async def total_leaderboard(ctx):
    """Displays the all-time leaderboard."""
    scores_df = get_scores_from_external_db()
    if scores_df.empty:
        await ctx.send("No scores available!")
        return
    
    # Sort by score
    all_time_scores = scores_df.sort_values(by='Score', ascending=False)
    
    # Create embed for leaderboard
    embed = discord.Embed(
        title="🏆 All-Time Leaderboard",
        description="Top scores across all time",
        color=discord.Color.purple()
    )
    
    # Add top 10 scores
    for i, (_, row) in enumerate(all_time_scores.head(10).iterrows()):
        username = row['Username'] if row['Username'] else "Unknown"
        score = row['Score']
        
        # Try to get Discord user if mapped
        discord_id = get_discord_id_from_twitch(username)
        if discord_id:
            embed.add_field(
                name=f"{i+1}. {username}",
                value=f"<@{discord_id}> • {score} points",
                inline=False
            )
        else:
            embed.add_field(
                name=f"{i+1}. {username}",
                value=f"{score} points",
                inline=False
            )
    
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    await ctx.send(embed=embed)

@bot.command()
@commands.has_role("Roll With Advantage!")
async def link_twitch(ctx, member: discord.Member, twitch_username: str):
    """
    Links a Discord user to a Twitch username:
    ?link_twitch @DiscordUser twitchUsername
    """
    if map_twitch_to_discord(twitch_username, str(member.id)):
        embed = discord.Embed(
            title="User Linked",
            description=f"Successfully linked {member.mention} to Twitch username '{twitch_username}'",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed)
    else:
        embed = discord.Embed(
            title="Error",
            description="Failed to link user. Database error.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)

@bot.command()
async def whoami(ctx):
    """
    Shows the Twitch username linked to the user's Discord account:
    ?whoami
    """
    twitch_username = get_twitch_from_discord_id(str(ctx.author.id))
    if twitch_username:
        embed = discord.Embed(
            title="Your Linked Account",
            description=f"{ctx.author.mention}, you are linked to Twitch username '{twitch_username}'",
            color=discord.Color.blue()
        )
    else:
        embed = discord.Embed(
            title="No Linked Account",
            description=f"{ctx.author.mention}, you are not linked to any Twitch username.\n"
                       f"An admin can link you with `?link_twitch @{ctx.author.name} your_twitch_username`",
            color=discord.Color.orange()
        )
    
    await ctx.send(embed=embed)

@bot.command()
async def member_count(ctx):
    """Shows the number of members in the guild."""
    guild = ctx.guild
    if guild:
        embed = discord.Embed(
            title="Server Members",
            description=f"I see {len(guild.members)} members in {guild.name}.",
            color=discord.Color.blue()
        )
        await ctx.send(embed=embed)
    else:
        await ctx.send("No guild found.")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def export_mappings(ctx):
    """
    Exports all user mappings to a CSV file:
    ?export_mappings
    """
    success, result = export_mappings_to_csv()
    if success:
        await ctx.send(f"✅ Successfully exported user mappings to: {result}")
        # Optionally send the file
        await ctx.send(file=discord.File(result))
    else:
        await ctx.send(f"❌ Failed to export mappings: {result}")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def import_mappings(ctx):
    """
    Imports user mappings from an attached CSV file:
    ?import_mappings (with attached CSV file)
    """
    if not ctx.message.attachments:
        await ctx.send("❌ Please attach a CSV file with the mappings.")
        return
    
    attachment = ctx.message.attachments[0]
    if not attachment.filename.endswith('.csv'):
        await ctx.send("❌ Attached file must be a CSV file.")
        return
    
    # Download the file
    try:
        await attachment.save(attachment.filename)
        success, message = import_mappings_from_csv(attachment.filename)
        
        if success:
            await ctx.send(f"✅ {message}")
        else:
            await ctx.send(f"❌ Import failed: {message}")
            
        # Clean up the file
        try:
            os.remove(attachment.filename)
        except:
            pass
    except Exception as e:
        await ctx.send(f"❌ Error processing the file: {str(e)}")

@bot.command(name="help")
async def custom_help(ctx):
    """A custom help command that lists all commands and usage."""
    embed = discord.Embed(
        title="Royal Scribe Commands",
        description="Here are the commands available in this server:",
        color=discord.Color.blue()
    )
    
    # User commands
    user_commands = """
**?leaderboard**
- Shows the current leaderboard.

**?total_leaderboard**
- Shows the all-time leaderboard.

**?whoami**
- Shows which Twitch username is linked to your Discord account.

**?member_count**
- Shows how many members the bot can see.

**?help**
- Shows this help message.
"""
    embed.add_field(name="User Commands", value=user_commands, inline=False)
    
    # Admin commands
    admin_commands = """
**?link_twitch @User TwitchUsername**
- Links a Discord user to a Twitch username.
- Admin-only: requires Roll With Advantage! role.

**?update_roles**
- Manually updates the champion roles.
- Admin-only: requires Roll With Advantage! role.

**?create_reaction_role Message Emoji @Role**
- Creates a new reaction role.
- Admin-only: requires Roll With Advantage! role.

**?create_role_message Title**
- Creates a reaction role message with pre-defined roles.
- Admin-only: requires Roll With Advantage! role.

**?export_mappings**
- Exports all Twitch-Discord user mappings to a CSV file.
- Admin-only: requires Roll With Advantage! role.

**?import_mappings**
- Imports Twitch-Discord mappings from an attached CSV file.
- Admin-only: requires Roll With Advantage! role.
"""
    embed.add_field(name="Admin Commands", value=admin_commands, inline=False)
    
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    await ctx.send(embed=embed)

# ------------------------------------------------------------
# 7) ROLE UPDATE LOGIC (CALLED AUTOMATICALLY AND MANUALLY)
# ------------------------------------------------------------
async def update_champion_roles():
    """
    Assigns 'Last Week's Champion' and 'All Time Champion' roles based on scores.
    """
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        logger.error("❌ Guild not found.")
        return

    scores_df = get_scores_from_external_db()
    if scores_df.empty:
        logger.info("No scores available, so no role updates.")
        return

    weekly_role = discord.utils.get(guild.roles, name=WEEKLY_CHAMPION_ROLE)
    all_time_role = discord.utils.get(guild.roles, name=ALL_TIME_CHAMPION_ROLE)
    
    if not weekly_role or not all_time_role:
        logger.error("❌ Could not find champion roles. Check role names!")
        return

    # --- Determine weekly champion from most recent data ---
    most_recent_date = scores_df['RecentDate'].max()
    recent_scores = scores_df[scores_df['RecentDate'] == most_recent_date].sort_values(by='Score', ascending=False)
    
    weekly_champion_twitch = None
    weekly_champion_discord_id = None
    
    if not recent_scores.empty:
        weekly_champion_twitch = recent_scores.iloc[0]['Username']
        if weekly_champion_twitch:
            weekly_champion_discord_id = get_discord_id_from_twitch(weekly_champion_twitch)

    # --- Determine all-time champion ---
    all_time_scores = scores_df.sort_values(by='Score', ascending=False)
    
    all_time_champion_twitch = None
    all_time_champion_discord_id = None
    
    if not all_time_scores.empty:
        all_time_champion_twitch = all_time_scores.iloc[0]['Username']
        if all_time_champion_twitch:
            all_time_champion_discord_id = get_discord_id_from_twitch(all_time_champion_twitch)

    # --- Remove champion roles from everyone who has them ---
    for member in guild.members:
        if weekly_role in member.roles:
            await member.remove_roles(weekly_role)
        if all_time_role in member.roles:
            await member.remove_roles(all_time_role)

    # --- Assign weekly champion role if we have a Discord user mapped ---
    if weekly_champion_discord_id:
        weekly_champion = guild.get_member(int(weekly_champion_discord_id))
        if weekly_champion:
            await weekly_champion.add_roles(weekly_role)
            logger.info(f"Assigned {WEEKLY_CHAMPION_ROLE} to {weekly_champion.name} (Twitch: {weekly_champion_twitch})")
        else:
            logger.warning(f"Could not find Discord user for ID {weekly_champion_discord_id} (Twitch: {weekly_champion_twitch})")
    else:
        logger.warning(f"No Discord mapping for weekly champion Twitch user: {weekly_champion_twitch}")

    # --- Assign all-time champion role if we have a Discord user mapped ---
    if all_time_champion_discord_id:
        all_time_champion = guild.get_member(int(all_time_champion_discord_id))
        if all_time_champion:
            await all_time_champion.add_roles(all_time_role)
            logger.info(f"Assigned {ALL_TIME_CHAMPION_ROLE} to {all_time_champion.name} (Twitch: {all_time_champion_twitch})")
        else:
            logger.warning(f"Could not find Discord user for ID {all_time_champion_discord_id} (Twitch: {all_time_champion_twitch})")
    else:
        logger.warning(f"No Discord mapping for all-time champion Twitch user: {all_time_champion_twitch}")

    logger.info("✅ Updated champion roles!")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def update_roles(ctx):
    """
    Manually trigger a role update with: ?update_roles
    """
    await update_champion_roles()
    embed = discord.Embed(
        title="Roles Updated",
        description="Champion roles have been updated manually!",
        color=discord.Color.green()
    )
    await ctx.send(embed=embed)

# ------------------------------------------------------------
# 8) REACTION ROLE COMMANDS AND EVENTS
# ------------------------------------------------------------
@bot.command()
@commands.has_role("Roll With Advantage!")
async def create_reaction_role(ctx, message_id: int, emoji: str, role: discord.Role):
    """
    Creates a new reaction role on an existing message:
    ?create_reaction_role 123456789 👍 @SomeRole
    """
    try:
        # Get the message
        message = await ctx.channel.fetch_message(message_id)
        
        # Add the reaction to the message
        await message.add_reaction(emoji)
        
        # Save to database
        if save_reaction_role(message_id, emoji, role.id):
            embed = discord.Embed(
                title="Reaction Role Created",
                description=f"React with {emoji} to get the {role.name} role.",
                color=discord.Color.green()
            )
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="Error",
                description="Failed to save reaction role to database.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
    except discord.NotFound:
        await ctx.send("❌ Message not found. Make sure you're using the right message ID.")
    except Exception as e:
        await ctx.send(f"❌ Error: {str(e)}")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def create_role_message(ctx, *, title="React to get roles!"):
    """
    Creates a new message with pre-defined reaction roles:
    ?create_role_message Choose your roles
    """
    # Create embed for reaction roles
    embed = discord.Embed(
        title=title,
        description="React to get roles:",
        color=discord.Color.blue()
    )
    
    for emoji, role_name in REACTION_ROLES.items():
        embed.add_field(name=f"{emoji} {role_name}", value="React to get this role.", inline=False)
    
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    
    # Send the message
    message = await ctx.send(embed=embed)
    
    # Add reactions and save to database
    for emoji in REACTION_ROLES.keys():
        await message.add_reaction(emoji)
        
        # Find role ID
        role = discord.utils.get(ctx.guild.roles, name=REACTION_ROLES[emoji])
        if role:
            # Save to database
            save_reaction_role(message.id, emoji, role.id)
        else:
            await ctx.send(f"⚠️ Warning: Role '{REACTION_ROLES[emoji]}' not found in server.")
    
    # Send success message once after all reactions are added
    success_embed = discord.Embed(
        title="✅ Role Message Created",
        description="The reaction role menu has been set up successfully!",
        color=discord.Color.green()
    )
    await ctx.send(embed=success_embed, delete_after=5.0)  # Auto-delete after 5 seconds

@bot.event
async def on_raw_reaction_add(payload):
    """Handles adding roles when users react to messages."""
    # Ignore bot reactions (must check if member exists first)
    if not payload.member or payload.member.bot:
        return
    
    # Get all reaction roles from the database
    reaction_roles = get_reaction_roles()
    
    # Check if this reaction is for a role
    message_id_str = str(payload.message_id)
    emoji_str = str(payload.emoji)
    
    if message_id_str in reaction_roles and emoji_str in reaction_roles[message_id_str]:
        role_id = int(reaction_roles[message_id_str][emoji_str])
        guild = bot.get_guild(payload.guild_id)
        
        if guild:
            role = guild.get_role(role_id)
            if role:
                await payload.member.add_roles(role)
                logger.info(f"Added {role.name} to {payload.member.name}")

@bot.event
async def on_raw_reaction_remove(payload):
    """Handles removing roles when users remove reactions."""
    # Get all reaction roles from the database
    reaction_roles = get_reaction_roles()
    
    # Check if this reaction is for a role
    message_id_str = str(payload.message_id)
    emoji_str = str(payload.emoji)
    
    if message_id_str in reaction_roles and emoji_str in reaction_roles[message_id_str]:
        role_id = int(reaction_roles[message_id_str][emoji_str])
        guild = bot.get_guild(payload.guild_id)
        
        if guild:
            member = guild.get_member(payload.user_id)
            if member and not member.bot:
                role = guild.get_role(role_id)
                if role:
                    await member.remove_roles(role)
                    logger.info(f"Removed {role.name} from {member.name}")

# ------------------------------------------------------------
# 9) SCHEDULED TASKS
# ------------------------------------------------------------
@tasks.loop(minutes=1)
async def schedule_weekly_update():
    """Checks if it's time to update champion roles (Sunday at 1 AM)."""
    now = datetime.now()
    # Sunday = 6, hour=1, minute=0
    if now.weekday() == 6 and now.hour == 1 and now.minute == 0:
        await update_champion_roles()
        # Optionally, send a message to a channel:
        channel = bot.get_channel(NOTIFICATION_CHANNEL_ID)
        if channel:
            embed = discord.Embed(
                title="Champion Roles Updated",
                description="Weekly champion roles have been updated!",
                color=discord.Color.gold()
            )
            await channel.send(embed=embed)

# ------------------------------------------------------------
# 10) ERROR HANDLING
# ------------------------------------------------------------
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        embed = discord.Embed(
            title="Permission Denied",
            description="Sorry, you can't do that! You're missing the required role.",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)
    elif isinstance(error, commands.CommandNotFound):
        embed = discord.Embed(
            title="Command Not Found",
            description=f"Command not found. Try `?help` for a list of commands.",
            color=discord.Color.orange()
        )
        await ctx.send(embed=embed)
    elif isinstance(error, commands.MissingRequiredArgument):
        embed = discord.Embed(
            title="Missing Argument",
            description=f"Missing required argument: {error.param.name}. Try `?help` for usage help.",
            color=discord.Color.yellow()
        )
        await ctx.send(embed=embed)
    else:
        logger.error(f"Command error: {error}")
        embed = discord.Embed(
            title="Error",
            description=f"An error occurred: {error}",
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)

# ------------------------------------------------------------
# 11) RUN THE BOT
# ------------------------------------------------------------
if __name__ == "__main__":
    # Attempt to create database tables before starting the bot
    create_tables_if_not_exist()
    
    # Run the bot with error handling
    try:
        bot.run(TOKEN)
    except Exception as e:
        logger.critical(f"Bot crashed: {e}")