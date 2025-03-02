import os
import pandas as pd
import discord
from discord.ext import commands, tasks
from datetime import datetime
import json
import matplotlib.pyplot as plt
import io
from dotenv import load_dotenv
import logging
import traceback

# ------------------------------------------------------------
# 1) CONFIGURATION SETUP - Using environment variables or config file
# ------------------------------------------------------------
load_dotenv()  # Load environment variables from .env file

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trivia_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("trivia_bot")

# Load config from a JSON file or use environment variables
try:
    with open('config.json', 'r') as f:
        config = json.load(f)
        TOKEN = config.get('TOKEN') or os.getenv("DISCORD_BOT_TOKEN")
        SCORES_FILE = config.get('SCORES_FILE') or os.getenv("SCORES_FILE", "trivia_scores.csv")
        GUILD_ID = config.get('GUILD_ID') or int(os.getenv("GUILD_ID", "747249327671476275"))
        MONTHLY_ROLE_NAME = config.get('MONTHLY_ROLE_NAME') or os.getenv("MONTHLY_ROLE_NAME", "Trivia Monthly Champion")
        ALL_TIME_ROLE_NAME = config.get('ALL_TIME_ROLE_NAME') or os.getenv("ALL_TIME_ROLE_NAME", "Trivia All Time Champion")
        ADMIN_ROLE_NAME = config.get('ADMIN_ROLE_NAME') or os.getenv("ADMIN_ROLE_NAME", "Roll With Advantage!")
except (FileNotFoundError, json.JSONDecodeError):
    # Fall back to environment variables if config file isn't available
    TOKEN = os.getenv("DISCORD_BOT_TOKEN")
    SCORES_FILE = os.getenv("SCORES_FILE", "trivia_scores.csv")
    GUILD_ID = int(os.getenv("GUILD_ID", "747249327671476275"))
    MONTHLY_ROLE_NAME = os.getenv("MONTHLY_ROLE_NAME", "Trivia Monthly Champion")
    ALL_TIME_ROLE_NAME = os.getenv("ALL_TIME_ROLE_NAME", "Trivia All Time Champion")
    ADMIN_ROLE_NAME = os.getenv("ADMIN_ROLE_NAME", "Roll With Advantage!")

# ------------------------------------------------------------
# 2) DISCORD INTENTS AND BOT SETUP
# ------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

# Create bot with prefix and help command override
bot = commands.Bot(command_prefix="?", intents=intents, help_command=None)

# ------------------------------------------------------------
# 3) CSV HELPER FUNCTIONS WITH ERROR HANDLING
# ------------------------------------------------------------
def load_scores():
    """Loads scores from CSV file with error handling."""
    try:
        # Force "User" column to be read as a string to avoid float precision issues
        df = pd.read_csv(SCORES_FILE, dtype={"User": str})
        # Clean up the DataFrame - remove empty rows
        df = df.dropna(how='all')
        return df
    except FileNotFoundError:
        logger.info(f"Scores file not found. Creating new file at {SCORES_FILE}")
        # If the file doesn't exist yet, return an empty dataframe with these columns
        df = pd.DataFrame(columns=["User", "Month", "Score"])
        df.to_csv(SCORES_FILE, index=False)
        return df
    except Exception as e:
        logger.error(f"Error loading scores: {e}")
        # Return empty DataFrame as fallback
        return pd.DataFrame(columns=["User", "Month", "Score"])

def save_scores(df):
    """Saves scores back to CSV with backup and error handling."""
    try:
        # Clean up the DataFrame before saving
        df = df.dropna(how='all')
        
        # Create a backup of the current file if it exists
        if os.path.exists(SCORES_FILE):
            backup_file = f"{SCORES_FILE}.bak"
            df_current = pd.read_csv(SCORES_FILE)
            df_current.to_csv(backup_file, index=False)
            
        # Save the updated dataframe
        df.to_csv(SCORES_FILE, index=False)
        return True
    except Exception as e:
        logger.error(f"Error saving scores: {e}")
        return False

def get_month_name(month_str):
    """Convert YYYY-MM to month name (e.g., '2025-02' to 'February 2025')"""
    try:
        year, month = month_str.split('-')
        return datetime.strptime(month, "%m").strftime("%B") + f" {year}"
    except:
        return month_str

# ------------------------------------------------------------
# 4) ON_READY EVENT (STARTS OUR SCHEDULED TASK)
# ------------------------------------------------------------
@bot.event
async def on_ready():
    # General info
    logger.info(f"✅ {bot.user} is now running!")
    logger.info(f"🌐 Connected to Discord as: {bot.user.name} (ID: {bot.user.id})")

    # Check your main guild
    guild = bot.get_guild(GUILD_ID)
    if guild:
        logger.info(f"Guild: {guild.name} has {len(guild.members)} members from the bot's view.")
    else:
        logger.error(f"❌ Guild not found with GUILD_ID = {GUILD_ID}")

    # Start your scheduled task if not already running
    if not schedule_weekly_update.is_running():
        schedule_weekly_update.start()
        logger.info("Weekly update scheduler started")

# ------------------------------------------------------------
# 5) TRIVIA COMMANDS
# ------------------------------------------------------------
@bot.command()
@commands.has_role(ADMIN_ROLE_NAME)
async def add_score(ctx, member: discord.Member, score: int):
    """
    Adds a score for a user, e.g.:
    ?add_score @SomeUser 5
    """
    df = load_scores()
    current_month = datetime.now().strftime("%Y-%m")

    user_id_str = str(member.id)

    # Check if user already has a score this month
    if ((df["User"] == user_id_str) & (df["Month"] == current_month)).any():
        df.loc[(df["User"] == user_id_str) & (df["Month"] == current_month), "Score"] += score
    else:
        new_row = pd.DataFrame({
            "User": [user_id_str],
            "Month": [current_month],
            "Score": [score]
        })
        df = pd.concat([df, new_row], ignore_index=True)

    if save_scores(df):
        await ctx.send(f"✅ Added {score} points to {member.mention} for {current_month}!")
        logger.info(f"Added {score} points to {member.name} (ID: {member.id}) by {ctx.author.name}")
    else:
        await ctx.send("❌ There was an error saving the scores. Please try again.")

@bot.command()
@commands.has_role(ADMIN_ROLE_NAME)
async def remove_score(ctx, member: discord.Member, score: int):
    """
    Removes points from a user, e.g.:
    ?remove_score @SomeUser 5
    """
    df = load_scores()
    current_month = datetime.now().strftime("%Y-%m")

    user_id_str = str(member.id)

    # Check if user already has a score this month
    if ((df["User"] == user_id_str) & (df["Month"] == current_month)).any():
        current_score = df.loc[(df["User"] == user_id_str) & (df["Month"] == current_month), "Score"].values[0]
        new_score = max(0, current_score - score)  # Prevent negative scores
        df.loc[(df["User"] == user_id_str) & (df["Month"] == current_month), "Score"] = new_score
        
        if save_scores(df):
            await ctx.send(f"✅ Removed {score} points from {member.mention}. New score: {new_score}")
            logger.info(f"Removed {score} points from {member.name} (ID: {member.id}) by {ctx.author.name}")
        else:
            await ctx.send("❌ There was an error saving the scores. Please try again.")
    else:
        await ctx.send(f"❌ {member.mention} doesn't have any points for {current_month} yet!")

@bot.command()
@commands.has_role(ADMIN_ROLE_NAME)
async def set_score(ctx, member: discord.Member, score: int):
    """
    Sets the score for a user to a specific value, e.g.:
    ?set_score @SomeUser 10
    """
    df = load_scores()
    current_month = datetime.now().strftime("%Y-%m")

    user_id_str = str(member.id)

    # Check if user already has a score this month
    if ((df["User"] == user_id_str) & (df["Month"] == current_month)).any():
        df.loc[(df["User"] == user_id_str) & (df["Month"] == current_month), "Score"] = score
    else:
        new_row = pd.DataFrame({
            "User": [user_id_str],
            "Month": [current_month],
            "Score": [score]
        })
        df = pd.concat([df, new_row], ignore_index=True)

    if save_scores(df):
        await ctx.send(f"✅ Set {member.mention}'s score to {score} for {current_month}!")
        logger.info(f"Set {member.name}'s (ID: {member.id}) score to {score} by {ctx.author.name}")
    else:
        await ctx.send("❌ There was an error saving the scores. Please try again.")

@bot.command()
async def member_count(ctx):
    """Shows the number of members in the server."""
    guild = ctx.guild
    if guild:
        await ctx.send(f"I see {len(guild.members)} members in {guild.name}.")
    else:
        await ctx.send("No guild found.")

@bot.command()
async def leaderboard(ctx, month=None):
    """
    Displays the leaderboard for a specific month (with mentions).
    Usage: ?leaderboard [YYYY-MM]
    Example: ?leaderboard 2025-02
    If no month is provided, shows current month.
    """
    df = load_scores()
    if df.empty:
        await ctx.send("No scores recorded yet!")
        return
        
    # Default to current month if none specified
    if not month:
        month = datetime.now().strftime("%Y-%m")
    
    # Validate month format
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        await ctx.send("❌ Invalid month format! Please use YYYY-MM (e.g., 2025-02)")
        return
    
    monthly_scores = (
        df[df["Month"] == month]
        .groupby("User")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Score", ascending=False)
    )
    
    if monthly_scores.empty:
        await ctx.send(f"No scores recorded for {get_month_name(month)}!")
        return

    # Get the readable month name
    month_name = get_month_name(month)
    
    # Create leaderboard embed
    embed = discord.Embed(
        title=f"🏆 {month_name} Trivia Leaderboard 🏆",
        color=0x00BFFF
    )
    
    # Add fields for top scorers with medals
    medals = ["🥇", "🥈", "🥉"]
    leaderboard_text = ""
    
    for idx, (_, row) in enumerate(monthly_scores.iterrows()):
        user_id = int(row["User"])
        score = row["Score"]
        
        member = ctx.guild.get_member(user_id)
        
        if not member:
            try:
                member = await ctx.guild.fetch_member(user_id)
            except discord.NotFound:
                member = None
        
        # Determine rank emoji
        rank_emoji = medals[idx] if idx < len(medals) else "🎮"
        
        if member:
            leaderboard_text += f"{rank_emoji} {member.mention}: **{score}** points\n"
        else:
            leaderboard_text += f"{rank_emoji} <@{user_id}>: **{score}** points\n"
    
    embed.description = leaderboard_text
    
    # Add footer
    embed.set_footer(text=f"Use '?leaderboard YYYY-MM' to view other months")
    
    await ctx.send(embed=embed)

@bot.command()
async def total_leaderboard(ctx):
    """Displays the all-time leaderboard (with mentions and ranking)."""
    df = load_scores()
    if df.empty:
        await ctx.send("No scores recorded yet!")
        return

    total_scores = (
        df.groupby("User")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Score", ascending=False)
    )

    # Create embed
    embed = discord.Embed(
        title="🌟 All-Time Trivia Leaderboard 🌟",
        color=0xFFD700
    )
    
    # Add fields for top scorers with medals
    medals = ["🥇", "🥈", "🥉"]
    leaderboard_text = ""
    
    for idx, (_, row) in enumerate(total_scores.iterrows()):
        user_id = int(row["User"])
        score = row["Score"]
        
        member = ctx.guild.get_member(user_id)
        if not member:
            try:
                member = await ctx.guild.fetch_member(user_id)
            except discord.NotFound:
                member = None
        
        # Determine rank emoji
        rank_emoji = medals[idx] if idx < len(medals) else "🎮"
        
        if member:
            leaderboard_text += f"{rank_emoji} {member.mention}: **{score}** points\n"
        else:
            leaderboard_text += f"{rank_emoji} <@{user_id}>: **{score}** points\n"
    
    embed.description = leaderboard_text
    
    await ctx.send(embed=embed)

@bot.command()
async def user_scores(ctx, member: discord.Member = None):
    """
    Shows scores for a specific user across all months.
    If no user is provided, shows the requester's scores.
    Usage: ?user_scores [@User]
    """
    if member is None:
        member = ctx.author
        
    df = load_scores()
    user_id_str = str(member.id)
    
    user_scores = df[df["User"] == user_id_str].sort_values(by="Month", ascending=False)
    
    if user_scores.empty:
        await ctx.send(f"No scores recorded for {member.mention}!")
        return
    
    # Create embed
    embed = discord.Embed(
        title=f"📊 Score History for {member.display_name}",
        color=member.color
    )
    
    # Group by month
    monthly_scores = (
        user_scores.groupby("Month")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Month", ascending=False)
    )
    
    # Add monthly scores to embed
    for _, row in monthly_scores.iterrows():
        month = row["Month"]
        score = row["Score"]
        embed.add_field(
            name=get_month_name(month),
            value=f"**{score}** points",
            inline=True
        )
    
    # Add total score
    total_score = user_scores["Score"].sum()
    embed.add_field(
        name="Total Score",
        value=f"**{total_score}** points",
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command()
async def stats(ctx):
    """Displays statistics and a chart of participation over time."""
    df = load_scores()
    if df.empty:
        await ctx.send("No scores recorded yet!")
        return
    
    # Generate basic stats
    total_months = df["Month"].nunique()
    total_users = df["User"].nunique()
    total_points = df["Score"].sum()
    
    # Create a plot of participation by month
    monthly_participation = df.groupby("Month")["User"].nunique().reset_index()
    monthly_participation = monthly_participation.sort_values(by="Month")
    
    # Create the plot
    plt.figure(figsize=(10, 6))
    plt.bar(monthly_participation["Month"], monthly_participation["User"], color='skyblue')
    plt.title("Trivia Participation by Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Participants")
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save plot to a bytes buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    
    # Create embed with stats
    embed = discord.Embed(
        title="📈 Trivia Statistics",
        color=0x1ABC9C
    )
    
    embed.add_field(name="Total Participants", value=f"**{total_users}** members", inline=True)
    embed.add_field(name="Total Months", value=f"**{total_months}** months", inline=True)
    embed.add_field(name="Total Points", value=f"**{total_points}** points", inline=True)
    
    # Get top scorer of all time
    top_scorer = df.groupby("User")["Score"].sum().reset_index().sort_values(by="Score", ascending=False)
    if not top_scorer.empty:
        user_id = int(top_scorer.iloc[0]["User"])
        top_score = top_scorer.iloc[0]["Score"]
        
        member = ctx.guild.get_member(user_id)
        if not member:
            try:
                member = await ctx.guild.fetch_member(user_id)
            except discord.NotFound:
                member = None
                
        if member:
            embed.add_field(
                name="All-Time Top Scorer",
                value=f"**{member.mention}** with **{top_score}** points",
                inline=False
            )
    
    # Send the embed and the chart
    file = discord.File(buf, filename="participation_chart.png")
    embed.set_image(url="attachment://participation_chart.png")
    
    await ctx.send(embed=embed, file=file)

@bot.command(name="helpme", aliases=["help"])
async def custom_help(ctx):
    """
    A custom help command that lists all commands and usage.
    """
    embed = discord.Embed(
        title="📚 Trivia Bot Commands",
        description="Here are all the available commands:",
        color=0x2ECC71
    )
    
    # Admin commands
    admin_commands = (
        "**?add_score @User Points**\n"
        "• Adds points to a user's monthly score.\n"
        f"• Admin-only: requires {ADMIN_ROLE_NAME} role.\n\n"
        
        "**?remove_score @User Points**\n"
        "• Removes points from a user's monthly score.\n"
        f"• Admin-only: requires {ADMIN_ROLE_NAME} role.\n\n"
        
        "**?set_score @User Points**\n"
        "• Sets a user's score to a specific value for the current month.\n"
        f"• Admin-only: requires {ADMIN_ROLE_NAME} role.\n\n"
        
        "**?update_roles**\n"
        "• Manually updates the champion roles.\n"
        f"• Admin-only: requires {ADMIN_ROLE_NAME} role.\n"
    )
    embed.add_field(name="🔒 Admin Commands", value=admin_commands, inline=False)
    
    # User commands
    user_commands = (
        "**?leaderboard [YYYY-MM]**\n"
        "• Shows the leaderboard for a specific month.\n"
        "• If no month is provided, shows current month.\n\n"
        
        "**?total_leaderboard**\n"
        "• Shows the all-time leaderboard.\n\n"
        
        "**?user_scores [@User]**\n"
        "• Shows scores for a specific user across all months.\n"
        "• If no user is provided, shows your scores.\n\n"
        
        "**?stats**\n"
        "• Shows overall trivia statistics and participation.\n\n"
        
        "**?member_count**\n"
        "• Shows how many members the bot can see.\n\n"
        
        "**?helpme**\n"
        "• Displays this help message.\n"
    )
    embed.add_field(name="👥 User Commands", value=user_commands, inline=False)
    
    await ctx.send(embed=embed)

# ------------------------------------------------------------
# 6) ROLE UPDATE LOGIC (CALLED AUTOMATICALLY AND MANUALLY)
# ------------------------------------------------------------
async def update_champion_roles():
    """
    Assigns 'Trivia Monthly Champion' and 'Trivia All Time Champion' 
    roles to the top scorers. Removes those roles from others.
    """
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        logger.error("❌ Guild not found.")
        return False

    df = load_scores()
    if df.empty:
        logger.info("No scores yet, so no role updates.")
        return False

    monthly_role = discord.utils.get(guild.roles, name=MONTHLY_ROLE_NAME)
    all_time_role = discord.utils.get(guild.roles, name=ALL_TIME_ROLE_NAME)
    
    if not monthly_role or not all_time_role:
        logger.error("❌ Could not find champion roles. Check role names!")
        return False

    # --- Determine top monthly scorer ---
    current_month = datetime.now().strftime("%Y-%m")
    monthly_scores = (
        df[df["Month"] == current_month]
        .groupby("User")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Score", ascending=False)
    )
    top_monthly_id = None
    if not monthly_scores.empty:
        top_monthly_id = int(monthly_scores.iloc[0]["User"])

    # --- Determine top all-time scorer ---
    total_scores = (
        df.groupby("User")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Score", ascending=False)
    )
    top_all_time_id = int(total_scores.iloc[0]["User"])

    # --- Remove champion roles from everyone who has them ---
    for member in guild.members:
        try:
            if monthly_role in member.roles:
                await member.remove_roles(monthly_role)
                logger.info(f"Removed monthly champion role from {member.name}")
            if all_time_role in member.roles:
                await member.remove_roles(all_time_role)
                logger.info(f"Removed all-time champion role from {member.name}")
        except discord.Forbidden:
            logger.error(f"Bot doesn't have permission to manage roles")
            return False
        except Exception as e:
            logger.error(f"Error updating roles for {member.name}: {e}")

    try:
        # --- Assign monthly champion role ---
        if top_monthly_id:
            monthly_champion = guild.get_member(top_monthly_id)
            if monthly_champion:
                await monthly_champion.add_roles(monthly_role)
                logger.info(f"Assigned monthly champion role to {monthly_champion.name}")
            else:
                logger.warning(f"Could not find monthly champion user with ID {top_monthly_id}")

        # --- Assign all-time champion role ---
        all_time_champion = guild.get_member(top_all_time_id)
        if all_time_champion:
            await all_time_champion.add_roles(all_time_role)
            logger.info(f"Assigned all-time champion role to {all_time_champion.name}")
        else:
            logger.warning(f"Could not find all-time champion user with ID {top_all_time_id}")
            
        logger.info("✅ Successfully updated champion roles!")
        return True
    except Exception as e:
        logger.error(f"Error assigning champion roles: {e}")
        return False

@bot.command()
@commands.has_role(ADMIN_ROLE_NAME)
async def update_roles(ctx):
    """
    Manually trigger a role update with: ?update_roles
    """
    success = await update_champion_roles()
    if success:
        await ctx.send("✅ Champion roles updated successfully!")
    else:
        await ctx.send("❌ There was an error updating the roles. Check the logs for details.")

# ------------------------------------------------------------
# 7) ERROR HANDLING
# ------------------------------------------------------------
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send(f"Sorry, you need the `{ADMIN_ROLE_NAME}` role to use this command.")
    elif isinstance(error, commands.MemberNotFound):
        await ctx.send("❌ Member not found. Please make sure you've mentioned a valid user.")
    elif isinstance(error, commands.BadArgument):
        await ctx.send("❌ Invalid argument. Type `?helpme` to see correct command usage.")
    elif isinstance(error, commands.CommandNotFound):
        await ctx.send("❌ Command not found. Type `?helpme` to see available commands.")
    else:
        # Log the error for debugging
        logger.error(f"Unhandled error: {error}")
        logger.error(traceback.format_exc())
        await ctx.send("❌ An error occurred while processing your command.")

# ------------------------------------------------------------
# 8) SCHEDULED TASK: WEEKLY ROLE UPDATE
# ------------------------------------------------------------
@tasks.loop(hours=1)  # More efficient to check every hour instead of every minute
async def schedule_weekly_update():
    now = datetime.now()
    # Sunday = 6, hour=1
    if now.weekday() == 6 and now.hour == 1:
        logger.info("Performing scheduled weekly role update")
        success = await update_champion_roles()
        
        if success:
            # Optionally, send a message to a specific channel
            guild = bot.get_guild(GUILD_ID)
            if guild:
                # You could define this channel ID in the config
                # channel = bot.get_channel(YOUR_CHANNEL_ID)
                # if channel:
                #     await channel.send("Weekly champion roles have been updated!")
                pass

# ------------------------------------------------------------
# 9) RUN THE BOT
# ------------------------------------------------------------
def main():
    try:
        logger.info("Starting Trivia Bot...")
        bot.run(TOKEN)
    except Exception as e:
        logger.critical(f"Failed to start the bot: {e}")
        logger.critical(traceback.format_exc())

if __name__ == "__main__":
    main()