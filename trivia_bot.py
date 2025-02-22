import os
import pandas as pd
import discord
from discord.ext import commands, tasks
from datetime import datetime

# ------------------------------------------------------------
# 1) SET THESE VALUES FOR YOUR SETUP
# ------------------------------------------------------------
TOKEN = os.getenv("DISCORD_BOT_TOKEN")  # Environment variable for bot token
SCORES_FILE = r"A:\Roll With Advantage\Trivia\Scores\trivia_scores.csv"  # Path to your CSV
GUILD_ID = 747249327671476275 # Replace with the numeric ID of your server

# Role names (must match what you have in your Discord server)
MONTHLY_ROLE_NAME = "Trivia Monthly Champion"
ALL_TIME_ROLE_NAME = "Trivia All Time Champion"

# ------------------------------------------------------------
# 2) DISCORD INTENTS AND BOT SETUP
# ------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="?", intents=intents)

# ------------------------------------------------------------
# 3) CSV HELPER FUNCTIONS
# ------------------------------------------------------------
def load_scores():
    """Loads scores from CSV file."""
    try:
        # Force "User" column to be read as a string to avoid float precision issues
        return pd.read_csv(SCORES_FILE, dtype={"User": str})
    except FileNotFoundError:
        # If the file doesn't exist yet, return an empty dataframe with these columns
        return pd.DataFrame(columns=["User", "Month", "Score"])

def save_scores(df):
    """Saves scores back to CSV."""
    df.to_csv(SCORES_FILE, index=False)

# ------------------------------------------------------------
# 4) ON_READY EVENT (STARTS OUR SCHEDULED TASK)
# ------------------------------------------------------------
@bot.event
async def on_ready():
    # General info
    print(f"✅ {bot.user} is now running!")
    print(f"🌐 Connected to Discord as: {bot.user.name} (ID: {bot.user.id})")

    # Check your main guild
    guild = bot.get_guild(GUILD_ID)
    if guild:
        print(f"Guild: {guild.name} has {len(guild.members)} members from the bot's view.")
    else:
        print("❌ Guild not found with GUILD_ID =", GUILD_ID)

    # Start your scheduled task if not already running
    if not schedule_weekly_update.is_running():
        schedule_weekly_update.start()


# ------------------------------------------------------------
# 5) TRIVIA COMMANDS
# ------------------------------------------------------------
@bot.command()
@commands.has_role("Roll With Advantage!")
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

    save_scores(df)
    await ctx.send(f"✅ Added {score} points to {member.mention} for {current_month}!")

@bot.command()
async def member_count(ctx):
    guild = ctx.guild
    if guild:
        await ctx.send(f"I see {len(guild.members)} members in {guild.name}.")
    else:
        await ctx.send("No guild found.")

@bot.command()
async def leaderboard(ctx):
    """Displays the current month's leaderboard (with mentions)."""
    df = load_scores()
    current_month = datetime.now().strftime("%Y-%m")

    monthly_scores = (
        df[df["Month"] == current_month]
        .groupby("User")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Score", ascending=False)
    )

    leaderboard_text = "**🏆 Monthly Trivia Leaderboard 🏆**\n"

    for _, row in monthly_scores.iterrows():
        user_id = int(row["User"])
        score = row["Score"]
        
        print(f"[DEBUG] Looking up user_id={user_id}, score={score}...")  # <--- ADDED
        member = ctx.guild.get_member(user_id)

        if not member:
            print("[DEBUG] get_member() returned None. Trying fetch_member()...")  # <--- ADDED
            try:
                member = await ctx.guild.fetch_member(user_id)
                print(f"[DEBUG] fetch_member() found: {member}")  # <--- ADDED
            except discord.NotFound:
                member = None
                print(f"[DEBUG] fetch_member() could NOT find user_id={user_id}")  # <--- ADDED

        if member:
            print(f"[DEBUG] Found member: {member} ({member.id})")  # <--- ADDED
            leaderboard_text += f"🥇 {member.mention}: {score} points\n"
        else:
            print(f"[DEBUG] Could not find user_id={user_id} in this guild.")  # <--- ADDED
            leaderboard_text += f"🥇 <@{user_id}>: {score} points\n"

    if leaderboard_text == "**🏆 Monthly Trivia Leaderboard 🏆**\n":
        leaderboard_text = "No scores yet for this month!"

    await ctx.send(leaderboard_text)

@bot.command()
async def total_leaderboard(ctx):
    """Displays the all-time leaderboard (with mentions)."""
    df = load_scores()
    if df.empty:
        await ctx.send("No scores yet!")
        return

    total_scores = (
        df.groupby("User")["Score"]
        .sum()
        .reset_index()
        .sort_values(by="Score", ascending=False)
    )

    leaderboard_text = "**🌟 All-Time Trivia Leaderboard 🌟**\n"
    for _, row in total_scores.iterrows():
        user_id = int(row["User"])
        member = ctx.guild.get_member(user_id)
        score = row["Score"]
        if member:
            leaderboard_text += f"🥇 {member.mention}: {score} points\n"
        else:
            leaderboard_text += f"🥇 <@{user_id}>: {score} points\n"

    await ctx.send(leaderboard_text or "No scores yet!")

@bot.command(name="helpme")
async def custom_help(ctx):
    """
    A custom help command that lists all commands and usage.
    """
    help_text = (
        "**Trivia Bot Commands**\n\n"
        "**?add_score @User Points**\n"
        "• Adds points to a user’s monthly score.\n"
        "• Admin-only: requires Manage Roles or similar permissions.\n\n"
        
        "**?leaderboard**\n"
        "• Shows the current month’s leaderboard.\n"
        "• Everyone can use this.\n\n"
        
        "**?total_leaderboard**\n"
        "• Shows the all-time leaderboard.\n"
        "• Everyone can use this.\n\n"
        
        "**?update_roles**\n"
        "• Manually updates the champion roles.\n"
        "• Admin-only: requires Manage Roles.\n\n"
        
        "**?member_count**\n"
        "• Shows how many members the bot can see.\n"
        "• Everyone can use this.\n\n"
        
        "**?helpme**\n"
        "• Displays this help message.\n"
        "• Everyone can use this.\n"
    )

    await ctx.send(help_text)
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
        print("❌ Guild not found.")
        return

    df = load_scores()
    if df.empty:
        print("No scores yet, so no role updates.")
        return

    monthly_role = discord.utils.get(guild.roles, name=MONTHLY_ROLE_NAME)
    all_time_role = discord.utils.get(guild.roles, name=ALL_TIME_ROLE_NAME)
    
    if not monthly_role or not all_time_role:
        print("❌ Could not find champion roles. Check role names!")
        return

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
        if monthly_role in member.roles:
            await member.remove_roles(monthly_role)
        if all_time_role in member.roles:
            await member.remove_roles(all_time_role)

    # --- Assign monthly champion role ---
    if top_monthly_id:
        monthly_champion = guild.get_member(top_monthly_id)
        if monthly_champion:
            await monthly_champion.add_roles(monthly_role)

    # --- Assign all-time champion role ---
    all_time_champion = guild.get_member(top_all_time_id)
    if all_time_champion:
        await all_time_champion.add_roles(all_time_role)

    print("✅ Auto-updated champion roles!")

@bot.command()
@commands.has_role("Roll With Advantage!")
async def update_roles(ctx):
    """
    Manually trigger a role update with: ?update_roles
    """
    await update_champion_roles()
    await ctx.send("Roles updated manually!")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.MissingRole):
        await ctx.send("Sorry, you can’t do that! You’re missing the required role.")
    else:
        raise error  # re-raise for any other errors we aren't handling

# ------------------------------------------------------------
# 7) SCHEDULED TASK: EVERY MINUTE, CHECK IF IT'S SUNDAY AT 1 AM
# ------------------------------------------------------------
@tasks.loop(minutes=1)
async def schedule_weekly_update():
    now = datetime.now()
    # Sunday = 6, hour=1, minute=0
    if now.weekday() == 6 and now.hour == 1 and now.minute == 0:
        await update_champion_roles()
        # Optionally, send a message to a channel:
        # channel = bot.get_channel(847646078513840209)
        # await channel.send("Weekly champion roles have been updated!")

# ------------------------------------------------------------
# 8) RUN THE BOT
# ------------------------------------------------------------
bot.run(TOKEN)
