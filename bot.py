import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler, MessageHandler, filters
import pymongo
from tmdbv3api import TMDb, Movie, TV, Search
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, UTC
import asyncio
import config
import logging
import re

# Setup logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Initialize TMDb API
tmdb = TMDb()
tmdb.api_key = config.TMDB_API_KEY
tmdb.language = 'en'
tmdb.debug = True
movie_api = Movie()
tv_api = TV()
search_api = Search()

# MongoDB setup
client = pymongo.MongoClient(config.MONGO_URI)
db = client['movie_bot']
users_collection = db['users']
movies_collection = db['movies']
news_collection = db['news']

# States for conversation
GENRE, CONFIRM, FIND_NAME = range(3)

# Available genres with emojis
GENRES = {
    'bollywood': '🎬 Bollywood',
    'hollywood': '🎬 Hollywood',
    'tamil': '🎬 Tamil',
    'tollywood': '🎬 Tollywood',
    'gujarati': '🎬 Gujarati',
    'marathi': '🎬 Marathi',
    'bengali': '🎬 Bengali',
    'punjabi': '🎬 Punjabi',
    'malayalam': '🎬 Malayalam',
    'kannada': '🎬 Kannada',
    'korean': '📺 Korean Dramas',
    'indian': '📺 Indian Dramas',
    'webseries': '🌐 Web Series',
    'anime': '🎞️ Anime'
}

async def start(update, context):
    try:
        user_id = update.effective_user.id
        user = users_collection.find_one({'user_id': user_id})
        
        if user:
            await update.message.reply_photo(
                photo=config.WELCOME_IMAGE_URL,
                caption=(
                    f"🎉 Welcome back! You're set for: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '').replace('🎞️ ', '') for g in user['genres']])}\n"
                    "Explore with /upcoming or reset with /reset. 🍿"
                )
            )
            return ConversationHandler.END
        
        keyboard = []
        row = []
        for i, genre in enumerate(GENRES):
            row.append(InlineKeyboardButton(GENRES[genre], callback_data=genre))
            if (i + 1) % 2 == 0 or i == len(GENRES) - 1:
                keyboard.append(row)
                row = []
        keyboard.append([InlineKeyboardButton("✅ Done", callback_data="done")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_photo(
            photo=config.WELCOME_IMAGE_URL,
            caption=(
                "🎥 Movie Mastermind: Your ultimate movie & series guide! Get personalized upcoming releases, news, and more. 🎬🍿\n"
                "Pick your genres to start!"
            ),
            reply_markup=reply_markup
        )
        context.user_data['selected_genres'] = []
        return GENRE
    except Exception as e:
        logger.error(f"Error in start: {e}")
        await update.message.reply_text("🚨 Error! Try again.")
        return ConversationHandler.END

async def genre_selection(update, context):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "done":
            if not context.user_data['selected_genres']:
                await query.message.reply_text("⚠️ Pick at least one genre!")
                return GENRE
            keyboard = [
                [InlineKeyboardButton("✔️ Confirm", callback_data="confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(
                f"Selected: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '').replace('🎞️ ', '') for g in context.user_data['selected_genres']])}\nConfirm? ✅",
                reply_markup=reply_markup
            )
            return CONFIRM
        
        if data in context.user_data['selected_genres']:
            context.user_data['selected_genres'].remove(data)
        else:
            context.user_data['selected_genres'].append(data)
        
        keyboard = []
        row = []
        for i, genre in enumerate(GENRES):
            text = f"{GENRES[genre]} {'✅' if genre in context.user_data['selected_genres'] else ''}"
            row.append(InlineKeyboardButton(text, callback_data=genre))
            if (i + 1) % 2 == 0 or i == len(GENRES) - 1:
                keyboard.append(row)
                row = []
        keyboard.append([InlineKeyboardButton("✅ Done", callback_data="done")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_reply_markup(reply_markup=reply_markup)
        return GENRE
    except Exception as e:
        logger.error(f"Error in genre_selection: {e}")
        await query.message.reply_text("🚨 Error! Try again.")
        return GENRE

async def confirm_selection(update, context):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "cancel":
            context.user_data['selected_genres'] = []
            await query.message.reply_text("Cancelled. Start again with /start. 🎬")
            return ConversationHandler.END
        
        user_id = update.effective_user.id
        users_collection.update_one(
            {'user_id': user_id},
            {'$set': {
                'genres': context.user_data['selected_genres'],
                'created_at': datetime.now(UTC)
            }},
            upsert=True
        )
        
        await query.message.reply_text(
            f"🎉 Locked in: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '').replace('🎞️ ', '') for g in context.user_data['selected_genres']])}\n"
            "Get upcoming releases with /upcoming! 🍿"
        )
        context.user_data['selected_genres'] = []
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in confirm_selection: {e}")
        await query.message.reply_text("🚨 Error! Try again.")
        return ConversationHandler.END

async def reset(update, context):
    try:
        user_id = update.effective_user.id
        users_collection.delete_one({'user_id': user_id})
        await update.message.reply_text("🔄 Reset done! Set new genres with /start.")
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in reset: {e}")
        await update.message.reply_text("🚨 Error! Try again.")
        return ConversationHandler.END

async def stats(update, context):
    try:
        user_id = update.effective_user.id
        if str(user_id) != config.OWNER_ID:
            await update.message.reply_text("🔒 Admin-only command.")
            return
        
        total_users = users_collection.count_documents({})
        genres_count = {}
        for user in users_collection.find():
            for genre in user.get('genres', []):
                genres_count[genre] = genres_count.get(genre, 0) + 1
        
        stats_message = f"📊 Bot Stats\n\nTotal Users: {total_users}\n\nGenres:\n"
        for genre, count in genres_count.items():
            stats_message += f"{GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '').replace('🎞️ ', '')}: {count} users\n"
        
        await update.message.reply_text(stats_message)
    except Exception as e:
        logger.error(f"Error in stats: {e}")
        await update.message.reply_text("🚨 Error! Try again.")

async def broadcast(update, context):
    try:
        user_id = update.effective_user.id
        if str(user_id) != config.OWNER_ID:
            await update.message.reply_text("🔒 Admin-only command.")
            return
        
        if not context.args:
            await update.message.reply_text("📢 Provide a message to broadcast: /broadcast <message>")
            return
        
        message = ' '.join(context.args)
        users = users_collection.find()
        sent = 0
        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=f"📢 Admin Broadcast: {message}",
                    parse_mode="Markdown"
                )
                sent += 1
            except Exception as e:
                logger.error(f"Error broadcasting to {user['user_id']}: {e}")
        
        await update.message.reply_text(f"📢 Broadcast sent to {sent} users.")
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await update.message.reply_text("🚨 Error! Try again.")

async def upcoming(update, context):
    try:
        user_id = update.effective_user.id
        user = users_collection.find_one({'user_id': user_id})
        
        if not user or not user.get('genres'):
            await update.message.reply_text(
                "⚠️ No genres selected! Pick your favorites with /start to unlock releases. 🎬"
            )
            return
        
        genres = user['genres']
        movies = fetch_upcoming_movies(genres)
        
        if not movies:
            await update.message.reply_text("😔 No upcoming releases found.")
            return
        
        for movie in movies[:5]:  # Limit to 5
            share_button = [[InlineKeyboardButton("📤 Share", switch_inline_query=f"Check out {movie['title']} ({movie['release_date']})!")]]
            reply_markup = InlineKeyboardMarkup(share_button)
            await update.message.reply_photo(
                photo=movie['poster'],
                caption=(
                    f"🎥 **{movie['title']}**\n"
                    f"Genre: {movie['genre']}\n"
                    f"Release: {movie['release_date']}\n"
                    f"Overview: {movie['overview'][:200]}...\n"
                    f"Cast: {movie.get('cast', 'N/A')}\n"
                    f"Director: {movie.get('director', 'N/A')}\n"
                    f"Languages: {movie.get('languages', 'N/A')}"
                ),
                reply_markup=reply_markup
            )
    except Exception as e:
        logger.error(f"Error in upcoming: {e}")
        await update.message.reply_text("🚨 Error fetching releases.")

async def find_start(update, context):
    try:
        await update.message.reply_text("🔍 Send me the movie or series name to find details and news!")
        return FIND_NAME
    except Exception as e:
        logger.error(f"Error in find_start: {e}")
        await update.message.reply_text("🚨 Error! Try again.")
        return ConversationHandler.END

async def find_name(update, context):
    try:
        query = update.message.text.strip()
        if not query:
            await update.message.reply_text("⚠️ Please provide a valid name!")
            return FIND_NAME
        
        # Search TMDb
        results = search_api.multi(query)
        if not results:
            await update.message.reply_text(f"😔 No results for '{query}'.")
            return ConversationHandler.END
        
        result = results[0]
        media_type = getattr(result, 'media_type', 'unknown')
        details = movie_api.details(result.id) if media_type == 'movie' else tv_api.details(result.id)
        
        # Sanitize data
        title = str(getattr(details, 'title', getattr(details, 'name', 'Unknown'))).strip()
        overview = re.sub(r'[^\x00-\x7F]+', ' ', str(details.get('overview', 'No overview available'))).strip()
        if not overview:
            overview = 'No overview available'
        
        credits = getattr(details, 'credits', {})
        cast = ', '.join([c['name'] for c in credits.get('cast', [])[:3]]) if credits.get('cast') else 'N/A'
        director = next((c['name'] for c in credits.get('crew', []) if c['job'] == 'Director'), 'N/A')
        languages = ', '.join([l['english_name'] for l in details.get('spoken_languages', [])]) if details.get('spoken_languages') else 'N/A'
        
        # Fetch related news
        news_items = news_collection.find({
            'title': {'$regex': re.escape(query), '$options': 'i'}
        }).limit(3)
        
        news_text = "\n\n📰 **Related News**:\n"
        news_count = 0
        for news in news_items:
            news_text += f"- {news['title']} ([Read more]({news['link']}))\n"
            news_count += 1
        if news_count == 0:
            news_text = "\n\n📰 **Related News**: None found."
        
        share_button = [[InlineKeyboardButton("📤 Share", switch_inline_query=f"Check out {title}!")]]
        reply_markup = InlineKeyboardMarkup(share_button)
        
        await update.message.reply_photo(
            photo=f"https://image.tmdb.org/t/p/w500{result.poster_path}" if result.poster_path else "https://via.placeholder.com/500",
            caption=(
                f"🎥 **{title}**\n"
                f"Type: {'Movie' if media_type == 'movie' else 'Series'}\n"
                f"Release: {getattr(result, 'release_date', getattr(result, 'first_air_date', 'TBA'))}\n"
                f"Overview: {overview[:200]}...\n"
                f"Cast: {cast}\n"
                f"Director: {director}\n"
                f"Languages: {languages}{news_text}"
            ),
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in find_name: {e}")
        await update.message.reply_text("🚨 Error fetching details. Try again.")
        return ConversationHandler.END

async def find_cancel(update, context):
    await update.message.reply_text("🔍 Search cancelled. Use /find to try again.")
    return ConversationHandler.END

def fetch_upcoming_movies(genres):
    try:
        movies = []
        for genre in genres:
            if genre in ['bollywood', 'bengali']:
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language in ['hi', 'bn']:
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'hollywood':
                tmdb_movies = movie_api.upcoming(region='US')
                for m in tmdb_movies:
                    if m.original_language == 'en':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'tamil':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'ta':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'tollywood':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'te':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'gujarati':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'gu':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'marathi':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'mr':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'punjabi':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'pa':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'malayalam':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'ml':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'kannada':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'kn':
                        movie_data = fetch_movie_details(m, genre)
                        if movie_data:
                            movies.append(movie_data)
            elif genre == 'korean':
                tmdb_shows = tv_api.on_the_air()
                for s in tmdb_shows:
                    if getattr(s, 'original_language', '') == 'ko':
                        tv_data = fetch_tv_details(s, genre)
                        if tv_data:
                            movies.append(tv_data)
            elif genre == 'indian':
                tmdb_shows = tv_api.on_the_air(region='IN')
                for s in tmdb_shows:
                    if s.origin_country and 'IN' in s.origin_country:
                        tv_data = fetch_tv_details(s, genre)
                        if tv_data:
                            movies.append(tv_data)
            elif genre == 'webseries':
                tmdb_shows = tv_api.popular()
                for s in tmdb_shows:
                    tv_data = fetch_tv_details(s, genre)
                    if tv_data:
                        movies.append(tv_data)
            elif genre == 'anime':
                tmdb_shows = tv_api.popular(genre=16)  # TMDb genre ID 16 = Animation
                for s in tmdb_shows:
                    if s.origin_country and 'JP' in s.origin_country:  # Japanese anime
                        tv_data = fetch_tv_details(s, genre)
                        if tv_data:
                            movies.append(tv_data)
        
        # Remove duplicates
        seen = set()
        unique_movies = []
        for movie in movies:
            key = (movie['title'], movie['release_date'])
            if key not in seen:
                seen.add(key)
                unique_movies.append(movie)
        
        # Store in MongoDB
        for movie in unique_movies:
            movies_collection.update_one(
                {'title': movie['title'], 'release_date': movie['release_date']},
                {'$set': movie},
                upsert=True
            )
        
        return unique_movies
    except Exception as e:
        logger.error(f"Error in fetch_upcoming_movies: {e}")
        return []

def fetch_movie_details(movie, genre):
    try:
        if not hasattr(movie, 'id') or not movie.id:
            logger.warning(f"Skipping movie with missing ID: {movie}")
            return {}
        
        details = movie_api.details(movie.id)
        if not details or not hasattr(details, 'title'):
            logger.warning(f"Invalid movie data for ID {movie.id}")
            return {}
        
        credits = getattr(details, 'credits', {})
        cast = ', '.join([c['name'] for c in credits.get('cast', [])[:3]]) if credits.get('cast') else 'N/A'
        director = next((c['name'] for c in credits.get('crew', []) if c['job'] == 'Director'), 'N/A')
        languages = ', '.join([l['english_name'] for l in details.get('spoken_languages', [])]) if details.get('spoken_languages') else 'N/A'
        overview = re.sub(r'[^\x00-\x7F]+', ' ', str(details.get('overview', 'No overview available'))).strip()
        
        return {
            'title': str(details.title).strip(),
            'genre': GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '').replace('🎞️ ', ''),
            'release_date': details.release_date or 'TBA',
            'overview': overview or 'No overview available',
            'poster': f"https://image.tmdb.org/t/p/w500{details.poster_path}" if details.poster_path else "https://via.placeholder.com/500",
            'cast': cast,
            'director': director,
            'languages': languages
        }
    except Exception as e:
        logger.error(f"Error fetching movie details for ID {getattr(movie, 'id', 'unknown')}: {e}")
        return {}

def fetch_tv_details(show, genre):
    try:
        if not hasattr(show, 'id') or not show.id:
            logger.warning(f"Skipping show with missing ID: {show}")
            return {}
        
        details = tv_api.details(show.id)
        if not details or not hasattr(details, 'name'):
            logger.warning(f"Invalid TV data for ID {show.id}")
            return {}
        
        credits = getattr(details, 'credits', {})
        cast = ', '.join([c['name'] for c in credits.get('cast', [])[:3]]) if credits.get('cast') else 'N/A'
        director = next((c['name'] for c in credits.get('crew', []) if c['job'] == 'Director'), 'N/A')
        languages = ', '.join([l['english_name'] for l in details.get('spoken_languages', [])]) if details.get('spoken_languages') else 'N/A'
        overview = re.sub(r'[^\x00-\x7F]+', ' ', str(details.get('overview', 'No overview available'))).strip()
        
        return {
            'title': str(details.name).strip(),
            'genre': GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '').replace('🎞️ ', ''),
            'release_date': details.first_air_date or 'TBA',
            'overview': overview or 'No overview available',
            'poster': f"https://image.tmdb.org/t/p/w500{details.poster_path}" if details.poster_path else "https://via.placeholder.com/500",
            'cast': cast,
            'director': director,
            'languages': languages
        }
    except Exception as e:
        logger.error(f"Error fetching TV details for ID {getattr(show, 'id', 'unknown')}: {e}")
        return {}

def scrape_movie_news():
    try:
        url = "https://www.screendaily.com/news/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        news_items = []
        for article in soup.find_all('article', limit=10):
            title = article.find('h2')
            if not title:
                continue
            title_text = title.text.strip()
            if not any(keyword in title_text.lower() for keyword in ['cast', 'director', 'release', 'language']):
                continue
            link = article.find('a')['href']
            full_link = f"https://www.screendaily.com{link}" if link.startswith('/') else link
            news_items.append({
                'title': title_text,
                'link': full_link,
                'scraped_at': datetime.now(UTC)
            })
        
        for news in news_items:
            news_collection.update_one(
                {'title': news['title'], 'link': news['link']},
                {'$set': news},
                upsert=True
            )
        
        return news_items
    except Exception as e:
        logger.error(f"Error scraping news: {e}")
        return []

async def notify_users(context):
    try:
        # Notify users without genres daily
        no_genre_users = users_collection.find({'genres': {'$exists': False}})
        for user in no_genre_users:
            await context.bot.send_message(
                chat_id=user['user_id'],
                text="🎬 Hey! Pick your favorite genres with /start to get personalized movie updates! 🍿"
            )
        
        # Monthly and weekly notifications
        current_month = datetime.now(UTC).strftime('%Y-%m')
        upcoming_movies = movies_collection.find({
            '$or': [
                {'release_date': {'$regex': f'^{current_month}'}},  # This month
                {'release_date': {
                    '$gte': datetime.now(UTC).strftime('%Y-%m-%d'),
                    '$lte': (datetime.now(UTC) + timedelta(days=7)).strftime('%Y-%m-%d')
                }}  # Next 7 days
            ]
        })
        
        for movie in upcoming_movies:
            for user in users_collection.find({'genres': movie['genre'].lower()}):
                share_button = [[InlineKeyboardButton("📤 Share", switch_inline_query=f"Check out {movie['title']} ({movie['release_date']})!")]]
                reply_markup = InlineKeyboardMarkup(share_button)
                await context.bot.send_photo(
                    chat_id=user['user_id'],
                    photo=movie['poster'],
                    caption=(
                        f"📢 **{movie['title']}** {'this month' if movie['release_date'].startswith(current_month) else 'in 7 days'}!\n"
                        f"Genre: {movie['genre']}\n"
                        f"Release: {movie['release_date']}\n"
                        f"Overview: {movie['overview'][:200]}...\n"
                        f"Cast: {movie.get('cast', 'N/A')}\n"
                        f"Director: {movie.get('director', 'N/A')}\n"
                        f"Languages: {movie.get('languages', 'N/A')}"
                    ),
                    reply_markup=reply_markup
                )
        
        # News notifications
        news_items = scrape_movie_news()
        for news in news_items:
            for user in users_collection.find():
                await context.bot.send_message(
                    chat_id=user['user_id'],
                    text=(
                        f"📰 **Movie News**: {news['title']}\n"
                        f"Read more: {news['link']}"
                    ),
                    parse_mode="Markdown"
                )
    except Exception as e:
        logger.error(f"Error in notify_users: {e}")

async def error_handler(update, context):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text("🚨 Error! Try again later.")

def main():
    try:
        app = Application.builder().token(config.TELEGRAM_TOKEN).build()
        
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler('start', start),
                CommandHandler('find', find_start)
            ],
            states={
                GENRE: [CallbackQueryHandler(genre_selection)],
                CONFIRM: [CallbackQueryHandler(confirm_selection)],
                FIND_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, find_name)]
            },
            fallbacks=[CommandHandler('cancel', find_cancel)],
            per_chat=True,
            per_user=True
        )
        
        app.add_handler(conv_handler)
        app.add_handler(CommandHandler('upcoming', upcoming))
        app.add_handler(CommandHandler('reset', reset))
        app.add_handler(CommandHandler('stats', stats))
        app.add_handler(CommandHandler('broadcast', broadcast))
        app.add_error_handler(error_handler)
        
        # Schedule notifications
        app.job_queue.run_repeating(notify_users, interval=86400, first=10)
        
        app.run_polling()
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == '__main__':
    main()
