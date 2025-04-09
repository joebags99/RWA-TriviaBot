import os
import pandas as pd
import discord
print(f"Discord.py version: {discord.__version__}")
from discord.ext import commands, tasks
from discord.ui import Button, View, Select, Modal, TextInput
import logging
from datetime import datetime, timedelta
import asyncio
import requests
import json
import pymysql
import pymysql.cursors
from dotenv import load_dotenv
import feedparser  # Add this for YouTube RSS feed parsing
import time

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

# YouTube Configuration
YOUTUBE_CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID", "UCjoM3DM9R1dCqMBtFcDhMsw")  # Default is RWA channel
YOUTUBE_CHECK_INTERVAL = 30  # Check every 30 minutes
YOUTUBE_NOTIFICATION_CHANNEL_ID = 747249434542473218
YOUTUBE_VIEWER_ROLE_ID = 1358892978789421151
LAST_VIDEO_ID_FILE = "last_video_id.txt"

# Twitch API Configuration
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_CHANNELS = os.getenv("TWITCH_CHANNELS", "rollwithadvantage").split(",")  # Comma-separated list of channels to track
TWITCH_CHECK_INTERVAL = 5  # Check every 5 minutes
TWITCH_NOTIFICATION_CHANNEL_ID = int(os.getenv("TWITCH_NOTIFICATION_CHANNEL_ID", "747249434542473218"))
TWITCH_VIEWER_ROLE_ID = int(os.getenv("TWITCH_VIEWER_ROLE_ID", "1358892810559819777"))  # Replace with your role ID
TWITCH_LIVE_STATUS_FILE = "twitch_live_status.json"

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
            affected_rows = cursor.execute(query, params)
            
            if fetch:
                result = cursor.fetchall()
            else:
                # Return affected rows for non-fetch operations
                result = affected_rows
            
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
    
    # Create weekly score snapshots table
    score_snapshot_query = """
    CREATE TABLE IF NOT EXISTS score_snapshots (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        score INT NOT NULL,
        snapshot_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        snapshot_type VARCHAR(50) NOT NULL,
        INDEX (username, snapshot_type)
    )
    """
    
    if execute_db_query(user_mapping_query, fetch=False) is not None and \
       execute_db_query(reaction_roles_query, fetch=False) is not None and \
       execute_db_query(score_snapshot_query, fetch=False) is not None:
        logger.info("Database tables created successfully")
        return True
    
    logger.error("Failed to create database tables")
    return False

# Add functions to handle weekly snapshots
def take_score_snapshot(snapshot_type="weekly"):
    """Takes a snapshot of current scores for future comparison."""
    scores_df = get_scores_from_external_db()
    if scores_df.empty:
        logger.warning("No scores available for snapshot")
        return False
    
    # Get the right column names
    username_column = next((col for col in scores_df.columns if col.lower() == 'username'), None)
    score_column = next((col for col in scores_df.columns if col.lower() == 'score'), None)
    
    if not username_column or not score_column:
        logger.error(f"Missing required columns for snapshot. Available: {scores_df.columns.tolist()}")
        return False
    
    # Create a transaction to save all snapshots
    connection = get_db_connection()
    if not connection:
        return False
    
    try:
        with connection.cursor() as cursor:
            # Clear old snapshots of this type first
            cursor.execute(
                "DELETE FROM score_snapshots WHERE snapshot_type = %s", 
                (snapshot_type,)
            )
            
            # Insert new snapshots
            for _, row in scores_df.iterrows():
                username = row[username_column]
                score = row[score_column]
                
                cursor.execute(
                    "INSERT INTO score_snapshots (username, score, snapshot_type) VALUES (%s, %s, %s)",
                    (username, score, snapshot_type)
                )
        
        connection.commit()
        logger.info(f"Successfully created {snapshot_type} snapshot for {len(scores_df)} users")
        return True
    except Exception as e:
        logger.error(f"Error taking snapshot: {e}")
        connection.rollback()
        return False

def get_session_scores():
    """Gets scores for the current session by comparing with last snapshot."""
    # Get current scores
    current_scores = get_scores_from_external_db()
    if current_scores.empty:
        return pd.DataFrame()
    
    # Get the right column names
    username_column = next((col for col in current_scores.columns if col.lower() == 'username'), None)
    score_column = next((col for col in current_scores.columns if col.lower() == 'score'), None)
    
    if not username_column or not score_column:
        logger.error(f"Missing required columns. Available: {current_scores.columns.tolist()}")
        return pd.DataFrame()
    
    # Get snapshot scores
    query = """
    SELECT username, score, snapshot_date
    FROM score_snapshots
    WHERE snapshot_type = 'weekly'
    """
    
    results = execute_db_query(query)
    if not results:
        # No snapshot exists yet, return current scores as session scores
        current_scores['SessionScore'] = current_scores[score_column]
        return current_scores
    
    # Convert to DataFrame
    snapshot_df = pd.DataFrame(results)
    
    # Merge the dataframes on username
    merged_df = pd.merge(
        current_scores,
        snapshot_df,
        left_on=username_column,
        right_on='username',
        how='left'
    )
    
    # Fill missing snapshot scores with zeros
    merged_df['score'] = merged_df['score'].fillna(0)
    
    # Calculate session score (current - snapshot)
    merged_df['SessionScore'] = merged_df[score_column] - merged_df['score']
    
    # Make sure session scores aren't negative
    merged_df['SessionScore'] = merged_df['SessionScore'].apply(lambda x: max(0, x))
    
    # Add snapshot date for reference
    if not merged_df.empty and 'snapshot_date' in merged_df.columns:
        snapshot_date = merged_df['snapshot_date'].iloc[0]
        merged_df['SnapshotDate'] = snapshot_date
    
    return merged_df

def get_scores_from_external_db():
    """Gets scores from the external database."""
    query = """
    SELECT 
        UserId as id, 
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
    
    # Verify required columns exist
    if 'Score' not in df.columns or 'Username' not in df.columns:
        logger.error(f"Missing required columns. Available columns: {df.columns.tolist()}")
        
        # Try to fix column names if possible
        if 'score' in df.columns and 'Score' not in df.columns:
            df['Score'] = df['score']
        if 'username' in df.columns and 'Username' not in df.columns:
            df['Username'] = df['username']
    
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
        
    # Start YouTube checker task
    if not check_youtube_videos.is_running():
        check_youtube_videos.start()
        logger.info("YouTube notification checker started")

    # Start Twitch monitoring task
    if not check_twitch_streams.is_running():
        check_twitch_streams.start()
        logger.info("Twitch stream monitoring started")

# ------------------------------------------------------------
# 6) LEADERBOARD AND USER COMMANDS
# ------------------------------------------------------------
class LeaderboardView(View):
    def __init__(self, scores_df, is_total=False, page=0, page_size=10, score_column=None):
        super().__init__(timeout=600)
        self.scores_df = scores_df
        self.is_total = is_total
        self.page = page
        self.page_size = page_size
        self.custom_score_column = score_column
        self.max_pages = max(1, (len(self.scores_df) + self.page_size - 1) // self.page_size)
        logger.info(f"LeaderboardView: {len(scores_df)} scores, {self.max_pages} pages")

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.primary, row=0)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info("Previous button clicked")
        if self.page > 0:
            self.page -= 1
            # Update button states
            self.prev_button.disabled = (self.page <= 0)
            self.next_button.disabled = (self.page >= self.max_pages - 1)
            self.page_indicator.label = f"Page {self.page + 1}/{self.max_pages}"
            
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
            logger.info(f"Moved to previous page: {self.page+1}/{self.max_pages}")
        else:
            await interaction.response.defer()
            logger.info("Previous button: Already at first page")

    @discord.ui.button(label="Page 1/1", style=discord.ButtonStyle.secondary, disabled=True, row=0)
    async def page_indicator(self, interaction: discord.Interaction, button: discord.ui.Button):
        # This button is just a label and doesn't do anything
        await interaction.response.defer()

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.primary, row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info(f"Next button clicked. Page: {self.page+1}/{self.max_pages}")
        if self.page < self.max_pages - 1:
            self.page += 1
            # Update button states
            self.prev_button.disabled = (self.page <= 0)
            self.next_button.disabled = (self.page >= self.max_pages - 1)
            self.page_indicator.label = f"Page {self.page + 1}/{self.max_pages}"
            
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
            logger.info(f"Moved to next page: {self.page+1}/{self.max_pages}")
        else:
            await interaction.response.defer()
            logger.info("Next button: Already at last page")

    @discord.ui.button(label="Show All-Time Scores", style=discord.ButtonStyle.success, row=1)
    async def toggle_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.is_total = not self.is_total
        button.label = "Show Weekly Scores" if self.is_total else "Show All-Time Scores"
        self.page = 0
        
        # Update navigation buttons
        self.prev_button.disabled = True
        self.next_button.disabled = (self.max_pages <= 1)
        self.page_indicator.label = f"Page {self.page + 1}/{self.max_pages}"
        
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    def get_embed(self):
        start_idx = self.page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.scores_df))
        page_data = self.scores_df.iloc[start_idx:end_idx]
        
        # Determine title and color based on view type
        if self.custom_score_column == 'SessionScore':
            title = "📊 Session Leaderboard"
            color = discord.Color.teal()
            description = "Scores earned since the last snapshot"
        else:
            title = "🏆 All-Time Leaderboard" if self.is_total else "📜 Current Leaderboard"
            color = discord.Color.purple() if self.is_total else discord.Color.gold()
            description = "Top scores across all time" if self.is_total else "Current top scores"
        
        # Get date if available for weekly view
        if not self.is_total and 'RecentDate' in self.scores_df.columns:
            most_recent_date = self.scores_df['RecentDate'].max()
            description = f"Top scores as of {most_recent_date}"
        
        # Add snapshot date if available
        if 'SnapshotDate' in self.scores_df.columns and not self.scores_df.empty:
            snapshot_date = self.scores_df['SnapshotDate'].iloc[0]
            description = f"{description} (since {snapshot_date})"
        
        embed = discord.Embed(title=title, description=description, color=color)
        
        # Debug column information
        available_columns = self.scores_df.columns.tolist()
        
        # Determine which columns to use (case-insensitive matching)
        score_column = self.custom_score_column if self.custom_score_column else next((col for col in available_columns if col.lower() == 'score'), None)
        username_column = next((col for col in available_columns if col.lower() == 'username'), None)
        
        if not score_column or not username_column:
            embed.add_field(
                name="Error", 
                value=f"Could not determine score or username columns. Available columns: {', '.join(available_columns)}"
            )
            return embed
        
        # Add each entry to the embed
        for i, (_, row) in enumerate(page_data.iterrows()):
            rank = start_idx + i + 1
            username = row.get(username_column, "Unknown")
            score = row.get(score_column, 0)
            
            # Handle numeric usernames
            if isinstance(username, (int, float)):
                username = str(int(username))
            
            # Try to get Discord user if mapped
            discord_id = None
            if isinstance(username, str) and not username.isdigit():
                discord_id = get_discord_id_from_twitch(username)
            
            # Format the leaderboard entry
            if discord_id:
                embed.add_field(
                    name=f"{rank}. {username}",
                    value=f"<@{discord_id}> • {score} points",
                    inline=False
                )
            else:
                embed.add_field(
                    name=f"{rank}. {username}",
                    value=f"{score} points",
                    inline=False
                )
        
        embed.set_footer(text="Royal Scribe | Roll With Advantage")
        return embed

@bot.command()
async def leaderboard(ctx):
    """Displays the current leaderboard with interactive controls."""
    # Try to get scores from database or CSV
    scores_df = get_scores_from_external_db()
    
    if scores_df.empty:
        try:
            logger.info("Attempting to read directly from CSV file")
            if os.path.exists(SCORES_FILE):
                scores_df = pd.read_csv(SCORES_FILE)
            
            if scores_df.empty:
                await ctx.send("No scores available!")
                return
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            await ctx.send("No scores available!")
            return
    
    # Add debug info
    logger.info(f"Loaded {len(scores_df)} score entries")
    logger.info(f"Available columns: {scores_df.columns.tolist()}")
    
    # Convert timestamp strings to datetime if needed
    if 'RecentDate' in scores_df.columns and isinstance(scores_df['RecentDate'].iloc[0], str):
        scores_df['RecentDate'] = pd.to_datetime(scores_df['RecentDate'])
    
    # Get the most recent date by extracting just the date part (ignore time)
    if 'RecentDate' in scores_df.columns:
        # Add a date-only column for grouping
        scores_df['DateOnly'] = scores_df['RecentDate'].dt.date
        most_recent_date = scores_df['DateOnly'].max()
        
        # Filter to most recent date (date part only)
        recent_scores = scores_df[scores_df['DateOnly'] == most_recent_date]
    else:
        # Fallback if RecentDate is not available
        recent_scores = scores_df
    
    # Sort and show all scores from the most recent date
    recent_scores = recent_scores.sort_values(by='Score', ascending=False)
    
    # Create interactive view
    view = LeaderboardView(recent_scores, is_total=False)
    
    # Send message with view
    await ctx.send(embed=view.get_embed(), view=view)

@bot.command()
async def total_leaderboard(ctx):
    """Displays the all-time leaderboard with interactive controls."""
    # Similar implementation to leaderboard, but with is_total=True
    scores_df = get_scores_from_external_db()
    
    if scores_df.empty:
        try:
            if os.path.exists(SCORES_FILE):
                scores_df = pd.read_csv(SCORES_FILE)
            
            if scores_df.empty:
                await ctx.send("No scores available!")
                return
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            await ctx.send("No scores available!")
            return
    
    # Process total scores
    try:
        if 'Month' in scores_df.columns:
            all_time_scores = scores_df.groupby('Username')['Score'].sum().reset_index()
        else:
            all_time_scores = scores_df
        
        all_time_scores = all_time_scores.sort_values(by='Score', ascending=False)
    except Exception as e:
        logger.error(f"Error processing scores: {e}")
        await ctx.send(f"Error processing scores: {str(e)}")
        return
    
    # Create interactive view
    view = LeaderboardView(all_time_scores, is_total=True)
    
    # Send message with view
    await ctx.send(embed=view.get_embed(), view=view)

@bot.command()
async def session_leaderboard(ctx):
    """Displays the leaderboard for the current session/week."""
    # Get session scores
    session_scores_df = get_session_scores()
    
    if session_scores_df.empty:
        await ctx.send("No session scores available!")
        return
    
    # Sort by session score
    if 'SessionScore' in session_scores_df.columns:
        session_scores_df = session_scores_df.sort_values(by='SessionScore', ascending=False)
    
    # Create interactive view
    view = LeaderboardView(session_scores_df, is_total=False, score_column='SessionScore')
    
    # Send message with view
    await ctx.send(embed=view.get_embed(), view=view)

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

@bot.command()
@commands.has_role("Roll With Advantage!")
async def take_snapshot(ctx):
    """Takes a snapshot of current scores for session tracking."""
    success = take_score_snapshot("weekly")
    
    if success:
        embed = discord.Embed(
            title="Snapshot Created",
            description="Successfully created a snapshot of current scores for session tracking.",
            color=discord.Color.green()
        )
    else:
        embed = discord.Embed(
            title="Error",
            description="Failed to create score snapshot.",
            color=discord.Color.red()
        )
    
    await ctx.send(embed=embed)

class HelpView(View):
    def __init__(self):
        super().__init__(timeout=180)  # 3 minute timeout
        self.current_page = "main"
    
    @discord.ui.button(label="User Commands", style=discord.ButtonStyle.primary)
    async def user_commands(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="User Commands",
            description="Commands available to all users",
            color=discord.Color.blue()
        )
        
        commands = {
            "?leaderboard": "Shows the current leaderboard with interactive controls.",
            "?total_leaderboard": "Shows the all-time leaderboard with interactive controls.",
            "?session_leaderboard": "Shows scores earned during the current session.",
            "?whoami": "Shows which Twitch username is linked to your Discord account.",
            "?member_count": "Shows how many members the bot can see.",
            "?help": "Shows this interactive help menu."
        }
        
        for cmd, desc in commands.items():
            embed.add_field(name=cmd, value=desc, inline=False)
        
        embed.set_footer(text="Royal Scribe | Roll With Advantage")
        self.current_page = "user"
        
        await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Admin Commands", style=discord.ButtonStyle.danger)
    async def admin_commands(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="Admin Commands",
            description="Commands only available to admins with the 'Roll With Advantage!' role",
            color=discord.Color.red()
        )
        
        commands = {
            "?link_twitch @User TwitchUsername": "Links a Discord user to a Twitch username.",
            "?update_roles": "Manually updates the champion roles.",
            "?create_role_message Title": "Creates an interactive role selection menu.",
            "?export_mappings": "Exports all Twitch-Discord user mappings to a CSV file.",
            "?import_mappings": "Imports Twitch-Discord mappings from an attached CSV file.",
            "?link_twitch_ui": "Opens an interactive UI for linking Discord users to Twitch usernames.",
            "?take_snapshot": "Takes a snapshot of current scores for session tracking.",
            "?check_youtube": "Manually checks for new YouTube videos and displays the latest video information.",
            "?twitch_status [ChannelName]": "Checks if a Twitch channel is currently live.",
            "?start_twitch_monitoring": "Manually starts Twitch stream monitoring."
        }
        
        for cmd, desc in commands.items():
            embed.add_field(name=cmd, value=desc, inline=False)
        
        embed.set_footer(text="Admin commands require the 'Roll With Advantage!' role")
        self.current_page = "admin"
        
        await interaction.response.edit_message(embed=embed, view=self)
    
    @discord.ui.button(label="Home", style=discord.ButtonStyle.secondary)
    async def home_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page == "main":
            await interaction.response.defer()
            return
        
        embed = discord.Embed(
            title="Royal Scribe Commands",
            description="Use the buttons below to navigate the help menu.",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="User Commands", 
            value="Commands available to all users", 
            inline=True
        )
        
        embed.add_field(
            name="Admin Commands", 
            value="Commands for server administrators", 
            inline=True
        )
        
        embed.set_footer(text="Royal Scribe | Roll With Advantage")
        self.current_page = "main"
        
        await interaction.response.edit_message(embed=embed, view=self)

@bot.command(name="help")
async def custom_help(ctx):
    """An interactive help command with buttons."""
    embed = discord.Embed(
        title="Royal Scribe Commands",
        description="Use the buttons below to navigate the help menu.",
        color=discord.Color.blue()
    )
    
    embed.add_field(
        name="User Commands", 
        value="Commands available to all users", 
        inline=True
    )
    
    embed.add_field(
        name="Admin Commands", 
        value="Commands for server administrators", 
        inline=True
    )
    
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    
    view = HelpView()
    await ctx.send(embed=embed, view=view)

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
class RoleSelectView(View):
    def __init__(self, roles_dict):
        super().__init__(timeout=None)  # No timeout for role selection
        self.add_item(RoleSelect(roles_dict))

class RoleSelect(discord.ui.Select):
    def __init__(self, roles_dict):
        options = []
        for emoji, role_name in roles_dict.items():
            options.append(discord.SelectOption(
                label=role_name,
                description=f"Get the {role_name} role",
                emoji=emoji,
                value=role_name
            ))
        
        super().__init__(
            placeholder="Select roles to toggle...",
            min_values=1,
            max_values=1,  # Let them select one at a time for clarity
            options=options,
            custom_id="role_select"
        )
    
    async def callback(self, interaction: discord.Interaction):
        # Get the selected role name
        selected_role_name = self.values[0]
        
        # Find the role in the server
        role = discord.utils.get(interaction.guild.roles, name=selected_role_name)
        
        if not role:
            await interaction.response.send_message(
                f"Error: Role '{selected_role_name}' not found.", 
                ephemeral=True
            )
            return
        
        # Toggle the role
        if role in interaction.user.roles:
            await interaction.user.remove_roles(role)
            await interaction.response.send_message(
                f"❌ Removed role: {role.name}", 
                ephemeral=True
            )
        else:
            await interaction.user.add_roles(role)
            await interaction.response.send_message(
                f"✅ Added role: {role.name}", 
                ephemeral=True
            )

@bot.command()
@commands.has_role("Roll With Advantage!")
async def create_role_message(ctx, *, title="Select your roles"):
    """Creates a new message with a dropdown menu for role selection."""
    embed = discord.Embed(
        title=title,
        description="Use the dropdown menu below to add or remove roles:",
        color=discord.Color.blue()
    )
    
    for emoji, role_name in REACTION_ROLES.items():
        embed.add_field(name=f"{emoji} {role_name}", 
                      value="Select to toggle this role", 
                      inline=True)
    
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    
    view = RoleSelectView(REACTION_ROLES)
    await ctx.send(embed=embed, view=view)
    
    # Confirmation message
    confirm_embed = discord.Embed(
        title="✅ Role Menu Created",
        description="The role selection menu has been set up successfully!",
        color=discord.Color.green()
    )
    await ctx.send(embed=confirm_embed, delete_after=5.0)

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
        # Take a weekly snapshot before updating roles
        take_score_snapshot("weekly")
        
        # Update champion roles
        await update_champion_roles()
        
        # Optionally, send a message to a channel:
        channel = bot.get_channel(NOTIFICATION_CHANNEL_ID)
        if channel:
            embed = discord.Embed(
                title="Weekly Update Complete",
                description="Champion roles have been updated and score snapshot taken!",
                color=discord.Color.gold()
            )
            await channel.send(embed=embed)

@tasks.loop(minutes=YOUTUBE_CHECK_INTERVAL)
async def check_youtube_videos():
    """Periodically check for new YouTube videos and send notifications."""
    logger.info("Checking for new YouTube videos...")
    
    try:
        latest_video = get_latest_youtube_video()
        last_video_id = get_last_video_id()
        
        if not latest_video:
            logger.warning("Could not fetch latest YouTube video information")
            return
        
        # If this is a new video (or first run with no saved ID)
        if latest_video['id'] != last_video_id:
            logger.info(f"New YouTube video detected: {latest_video['title']}")
            
            # Get the notification channel
            channel = bot.get_channel(YOUTUBE_NOTIFICATION_CHANNEL_ID)
            if not channel:
                logger.error(f"YouTube notification channel not found: {YOUTUBE_NOTIFICATION_CHANNEL_ID}")
                return
            
            # Create embed for the video
            embed = discord.Embed(
                title=latest_video['title'],
                url=latest_video['url'],
                description="New video from Roll With Advantage!",
                color=discord.Color.red()
            )
            embed.set_thumbnail(url=f"https://img.youtube.com/vi/{latest_video['id']}/maxresdefault.jpg")
            embed.add_field(name="Published", value=latest_video['published'], inline=False)
            embed.set_footer(text="Royal Scribe | Roll With Advantage")
            
            # Send notification with role mention
            await channel.send(
                f"Roll With Advantage has posted a new video, check it out! <@&{YOUTUBE_VIEWER_ROLE_ID}>",
                embed=embed
            )
            
            # Save the new video ID
            save_last_video_id(latest_video['id'])
        
    except Exception as e:
        logger.error(f"Error in YouTube notification task: {e}")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def check_youtube(ctx):
    """Manually check for new YouTube videos."""
    await ctx.send("Checking for new YouTube videos...")
    
    try:
        latest_video = get_latest_youtube_video()
        last_video_id = get_last_video_id()
        
        if not latest_video:
            await ctx.send("Could not fetch YouTube video information.")
            return
        
        # Create embed with video info for preview
        embed = discord.Embed(
            title=latest_video['title'],
            url=latest_video['url'],
            description="Latest video from Roll With Advantage",
            color=discord.Color.red()
        )
        embed.set_thumbnail(url=f"https://img.youtube.com/vi/{latest_video['id']}/maxresdefault.jpg")
        embed.add_field(name="Published", value=latest_video['published'], inline=False)
        embed.add_field(name="Video ID", value=latest_video['id'], inline=False)
        embed.add_field(name="Last Notified ID", value=last_video_id or "None", inline=False)
        embed.set_footer(text="Royal Scribe | Roll With Advantage")
        
        await ctx.send(embed=embed)
        
    except Exception as e:
        await ctx.send(f"Error checking YouTube: {str(e)}")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def test_feed(ctx):
    """Test the YouTube RSS feed parser directly."""
    await ctx.send("Testing YouTube RSS feed...")
    
    try:
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={YOUTUBE_CHANNEL_ID}"
        feed = feedparser.parse(rss_url)  # Remove timeout parameter
        
        if feed.entries and len(feed.entries) > 0:
            await ctx.send(f"Feed parsed successfully! Found {len(feed.entries)} videos.")
            
            # Get details of first entry for debugging
            first = feed.entries[0]
            
            # More detailed debug info
            debug_info = "First video details:\n"
            debug_info += f"- Title: {first.title if hasattr(first, 'title') else 'No title'}\n"
            debug_info += f"- ID: {first.id if hasattr(first, 'id') else 'No ID'}\n"
            debug_info += f"- Link: {first.link if hasattr(first, 'link') else 'No link'}\n"
            debug_info += f"- Published: {first.published if hasattr(first, 'published') else 'No date'}\n"
            
            await ctx.send(debug_info)
        else:
            await ctx.send("Feed parsed but no entries found.")
    except Exception as e:
        await ctx.send(f"Error testing feed: {str(e)}")

@tasks.loop(minutes=TWITCH_CHECK_INTERVAL)
async def check_twitch_streams():
    """Periodically check for Twitch streams going live or offline."""
    logger.info("Checking Twitch stream status...")
    
    # Load previous status
    previous_status = load_live_status()
    current_status = {}
    
    for channel in TWITCH_CHANNELS:
        try:
            stream_data = get_stream_status(channel)
            
            if not stream_data:
                logger.warning(f"Could not fetch Twitch stream data for {channel}")
                continue
            
            current_status[channel] = stream_data
            was_live = previous_status.get(channel, {}).get('is_live', False)
            now_live = stream_data.get('is_live', False)
            
            # Stream just went live
            if now_live and not was_live:
                logger.info(f"Twitch channel {channel} went live!")
                await send_stream_notification(channel, stream_data)
            
            # Stream just went offline
            elif was_live and not now_live:
                logger.info(f"Twitch channel {channel} went offline")
                # Optionally handle stream ended notifications
        
        except Exception as e:
            logger.error(f"Error processing Twitch channel {channel}: {e}")
    
    # Save current status
    save_live_status(current_status)

async def send_stream_notification(channel_name, stream_data):
    """Send a notification when a Twitch stream goes live."""
    notification_channel = bot.get_channel(TWITCH_NOTIFICATION_CHANNEL_ID)
    if not notification_channel:
        logger.error(f"Twitch notification channel not found: {TWITCH_NOTIFICATION_CHANNEL_ID}")
        return
    
    # Get channel info for profile image
    channel_info = get_channel_info(channel_name)
    profile_url = channel_info.get('profile_image_url') if channel_info else None
    
    # Create embed
    embed = discord.Embed(
        title=stream_data.get('title', f"{channel_name} is now live!"),
        url=f"https://twitch.tv/{channel_name}",
        description=f"Playing {stream_data.get('game_name', 'something awesome')}",
        color=0x6441A4  # Twitch purple
    )
    
    if profile_url:
        embed.set_author(name=f"{channel_name} is now live!", icon_url=profile_url)
    
    if stream_data.get('thumbnail_url'):
        embed.set_image(url=f"{stream_data.get('thumbnail_url')}?t={int(time.time())}")
    
    embed.add_field(name="Viewers", value=stream_data.get('viewer_count', 0), inline=True)
    embed.set_footer(text="Royal Scribe | Roll With Advantage")
    
    # Send notification with role mention
    if TWITCH_VIEWER_ROLE_ID:
        await notification_channel.send(
            f"**{channel_name}** is now live on Twitch! <@&{TWITCH_VIEWER_ROLE_ID}>",
            embed=embed
        )
    else:
        await notification_channel.send(
            f"**{channel_name}** is now live on Twitch!",
            embed=embed
        )

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
# 11) LINK TWITCH UI
# ------------------------------------------------------------
class LinkTwitchModal(Modal, title="Link Twitch Account"):
    def __init__(self, member):
        super().__init__()
        self.member = member
        
        self.twitch_username = TextInput(
            label="Twitch Username",
            placeholder="Enter the Twitch username...",
            required=True,
            max_length=50
        )
        
        self.add_item(self.twitch_username)
    
    async def on_submit(self, interaction):
        twitch_name = self.twitch_username.value
        success = map_twitch_to_discord(twitch_name, str(self.member.id))
        
        if success:
            embed = discord.Embed(
                title="User Linked",
                description=f"Successfully linked {self.member.mention} to Twitch username '{twitch_name}'",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="Error",
                description="Failed to link user. Database error.",
                color=discord.Color.red()
            )
        
        await interaction.response.send_message(embed=embed)

class LinkTwitchView(View):
    def __init__(self, ctx):
        super().__init__(timeout=60)
        self.ctx = ctx
    
    @discord.ui.button(label="Link Twitch Account", style=discord.ButtonStyle.primary)
    async def link_twitch_button(self, interaction, button):
        # Check if user has admin role
        if not any(role.name == "Roll With Advantage!" for role in interaction.user.roles):
            await interaction.response.send_message("You don't have permission to use this feature.", ephemeral=True)
            return
        
        # Create select menu for member selection
        options = []
        for member in interaction.guild.members:
            if not member.bot:
                options.append(discord.SelectOption(
                    label=member.display_name,
                    description=f"ID: {member.id}",
                    value=str(member.id)
                ))
        
        # Create a new view with member select
        select_view = View()
        select = Select(
            placeholder="Select a member to link...",
            options=options[:25],  # Discord limits to 25 options
            custom_id="member_select"
        )
        
        async def select_callback(interaction):
            member_id = select.values[0]
            member = interaction.guild.get_member(int(member_id))
            
            if member:
                modal = LinkTwitchModal(member)
                await interaction.response.send_modal(modal)
            else:
                await interaction.response.send_message("Member not found.", ephemeral=True)
        
        select.callback = select_callback
        select_view.add_item(select)
        
        await interaction.response.send_message("Select a member to link:", view=select_view, ephemeral=True)

@bot.command()
@commands.has_role("Roll With Advantage!")
async def link_twitch_ui(ctx):
    """Opens an interactive UI for linking Discord users to Twitch usernames."""
    view = LinkTwitchView(ctx)
    await ctx.send("Click the button below to link a Discord user to a Twitch username:", view=view)

# ------------------------------------------------------------
# 12) TWITCH API INTEGRATION
# ------------------------------------------------------------
def get_twitch_access_token():
    """Get OAuth access token from Twitch API."""
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logger.error("Twitch API credentials not configured")
        return None
    
    try:
        url = "https://id.twitch.tv/oauth2/token"
        
        # Use json parameter instead of data for proper content-type
        payload = {
            'client_id': TWITCH_CLIENT_ID,
            'client_secret': TWITCH_CLIENT_SECRET,
            'grant_type': 'client_credentials'
        }
        
        # Add explicit headers
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Log attempt (without exposing secret)
        logger.info(f"Attempting Twitch auth with client ID: {TWITCH_CLIENT_ID[:5]}...")
        
        # Use json parameter to ensure proper formatting
        response = requests.post(url, json=payload, headers=headers)
        
        # If error, get more detailed information
        if response.status_code != 200:
            logger.error(f"Twitch auth error {response.status_code}: {response.text}")
            response.raise_for_status()
        
        data = response.json()
        logger.info("Successfully obtained Twitch access token")
        return data.get('access_token')
    except Exception as e:
        logger.error(f"Error getting Twitch access token: {e}")
        return None

def get_stream_status(channel_name):
    """Check if a Twitch channel is currently streaming."""
    access_token = get_twitch_access_token()
    if not access_token:
        return None
    
    try:
        headers = {
            'Client-ID': TWITCH_CLIENT_ID,
            'Authorization': f'Bearer {access_token}'
        }
        
        url = f"https://api.twitch.tv/helix/streams?user_login={channel_name.lower()}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        streams = data.get('data', [])
        
        if streams and len(streams) > 0:
            # Stream is live
            stream_data = streams[0]
            return {
                'is_live': True,
                'title': stream_data.get('title', 'Untitled Stream'),
                'game_name': stream_data.get('game_name', 'Unknown Game'),
                'viewer_count': stream_data.get('viewer_count', 0),
                'started_at': stream_data.get('started_at'),
                'thumbnail_url': stream_data.get('thumbnail_url', '').replace('{width}', '1280').replace('{height}', '720'),
                'user_name': stream_data.get('user_name', channel_name)
            }
        else:
            # Stream is offline
            return {'is_live': False}
    except Exception as e:
        logger.error(f"Error checking Twitch stream status: {e}")
        return None

def get_channel_info(channel_name):
    """Get detailed info about a Twitch channel."""
    access_token = get_twitch_access_token()
    if not access_token:
        return None
    
    try:
        headers = {
            'Client-ID': TWITCH_CLIENT_ID,
            'Authorization': f'Bearer {access_token}'
        }
        
        url = f"https://api.twitch.tv/helix/users?login={channel_name.lower()}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        users = data.get('data', [])
        
        if users and len(users) > 0:
            return users[0]
        else:
            logger.warning(f"No channel info found for {channel_name}")
            return None
    except Exception as e:
        logger.error(f"Error getting Twitch channel info: {e}")
        return None

def load_live_status():
    """Load the saved live status of Twitch channels."""
    try:
        if os.path.exists(TWITCH_LIVE_STATUS_FILE):
            with open(TWITCH_LIVE_STATUS_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading Twitch live status: {e}")
        return {}

def save_live_status(status_dict):
    """Save the live status of Twitch channels."""
    try:
        with open(TWITCH_LIVE_STATUS_FILE, 'w') as f:
            json.dump(status_dict, f)
        return True
    except Exception as e:
        logger.error(f"Error saving Twitch live status: {e}")
        return False

# ------------------------------------------------------------
# 13) YOUTUBE NOTIFICATION FUNCTIONS
# ------------------------------------------------------------
def get_last_video_id():
    """Get the ID of the last notified YouTube video."""
    try:
        if os.path.exists(LAST_VIDEO_ID_FILE):
            with open(LAST_VIDEO_ID_FILE, 'r') as f:
                return f.read().strip()
        return None
    except Exception as e:
        logger.error(f"Error reading last video ID: {e}")
        return None

def save_last_video_id(video_id):
    """Save the ID of the last notified YouTube video."""
    try:
        with open(LAST_VIDEO_ID_FILE, 'w') as f:
            f.write(video_id)
        return True
    except Exception as e:
        logger.error(f"Error saving last video ID: {e}")
        return False

def get_latest_youtube_video():
    """Get the latest YouTube video information from the channel's RSS feed."""
    try:
        logger.info(f"Fetching YouTube RSS feed for channel: {YOUTUBE_CHANNEL_ID}")
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={YOUTUBE_CHANNEL_ID}"
        
        # Remove the timeout parameter that's causing errors
        feed = feedparser.parse(rss_url)
        
        logger.info(f"Feed entries found: {len(feed.entries)}")
        
        if feed.entries and len(feed.entries) > 0:
            latest_video = feed.entries[0]
            
            # Debug info
            logger.info(f"Available attributes: {dir(latest_video)}")
            
            # Standard YouTube ID extraction from feed entry ID 
            # Format is typically "yt:video:VIDEO_ID"
            if hasattr(latest_video, 'id'):
                video_id = latest_video.id.split(':')[-1]
            else:
                logger.error("No ID field in the feed entry")
                return None
                
            video_title = latest_video.title if hasattr(latest_video, 'title') else "Unknown Title"
            video_url = latest_video.link if hasattr(latest_video, 'link') else f"https://www.youtube.com/watch?v={video_id}"
            video_published = latest_video.published if hasattr(latest_video, 'published') else "Unknown Date"
            
            logger.info(f"Successfully fetched video: {video_title} ({video_id})")
            
            return {
                'id': video_id,
                'title': video_title,
                'url': video_url,
                'published': video_published
            }
        else:
            logger.warning("No entries found in YouTube RSS feed")
            return None
    except Exception as e:
        logger.error(f"Error fetching YouTube RSS: {e}", exc_info=True)
        return None

# ------------------------------------------------------------
# 14) TWITCH STATUS COMMANDS
# ------------------------------------------------------------
@bot.command()
async def twitch_status(ctx, channel_name=None):
    """Check if a Twitch channel is currently live."""
    # Use default channel if none provided
    if not channel_name:
        if not TWITCH_CHANNELS:
            await ctx.send("No Twitch channels configured!")
            return
        channel_name = TWITCH_CHANNELS[0]
    
    await ctx.send(f"Checking stream status for {channel_name}...")
    
    stream_data = get_stream_status(channel_name)
    if not stream_data:
        await ctx.send(f"Could not fetch stream data for {channel_name}.")
        return
    
    if stream_data.get('is_live', False):
        # Create embed for live stream
        embed = discord.Embed(
            title=stream_data.get('title', f"{channel_name} is live!"),
            url=f"https://twitch.tv/{channel_name}",
            description=f"Playing {stream_data.get('game_name', 'something awesome')}",
            color=0x6441A4  # Twitch purple
        )
        
        if stream_data.get('thumbnail_url'):
            embed.set_image(url=f"{stream_data.get('thumbnail_url')}?t={int(time.time())}")
        
        embed.add_field(name="Viewers", value=stream_data.get('viewer_count', 0), inline=True)
        embed.add_field(name="Started At", value=stream_data.get('started_at', 'Unknown'), inline=True)
        embed.set_footer(text="Royal Scribe | Roll With Advantage")
        
        await ctx.send(f"**{channel_name}** is currently LIVE!", embed=embed)
    else:
        await ctx.send(f"**{channel_name}** is currently offline.")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def start_twitch_monitoring(ctx):
    """Manually start the Twitch stream monitoring."""
    if not check_twitch_streams.is_running():
        check_twitch_streams.start()
        await ctx.send("✅ Twitch stream monitoring started!")
    else:
        await ctx.send("Twitch stream monitoring is already running.")

# ------------------------------------------------------------
# 15) RUN THE BOT
# ------------------------------------------------------------
if __name__ == "__main__":
    # Check for existing instance using file locking
    import sys
    import fcntl  # For Linux/Unix
    
    try:
        # Try to create and lock a file
        lock_file = open("trivia_bot.lock", "w")
        
        try:
            # For Windows
            import msvcrt
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            is_locked = True
        except ImportError:
            # For Unix/Linux
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                is_locked = True
            except IOError:
                is_locked = False
        
        if not is_locked:
            logger.critical("Another instance of the bot is already running! Exiting.")
            sys.exit(1)
            
        logger.info("No other instances detected. Starting bot...")
        
        # Attempt to create database tables before starting the bot
        create_tables_if_not_exist()
        
        # Run the bot with error handling
        try:
            bot.run(TOKEN)
        except Exception as e:
            logger.critical(f"Bot crashed: {e}")
            
    except IOError:
        logger.critical("Could not create lock file. Another instance may be running.")
        sys.exit(1)