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
GENRE, CONFIRM = range(2)

# Available genres with emojis
GENRES = {
    'bollywood': '🎬 Bollywood',
    'hollywood': '🎬 Hollywood',
    'tamil': '🎬 Tamil',
    'tollywood': '🎬 Tollywood',
    'gujarati': '🎬 Gujarati',
    'marathi': '🎬 Marathi',
    'korean': '📺 Korean Dramas',
    'indian': '📺 Indian Dramas',
    'webseries': '🌐 Web Series'
}

async def start(update, context):
    try:
        user_id = update.effective_user.id
        user = users_collection.find_one({'user_id': user_id})
        
        if user:
            await update.message.reply_text(
                f"🎉 Welcome back! You're set for: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '') for g in user['genres']])}\n"
                "Explore with /upcoming or reset with /reset. 🍿"
            )
            return ConversationHandler.END
        
        keyboard = []
        row = []
        for i, genre in enumerate(GENRES):
            row.append(InlineKeyboardButton(GENRES[genre], callback_data=genre))
            if (i + 1) % 3 == 0 or i == len(GENRES) - 1:
                keyboard.append(row)
                row = []
        keyboard.append([InlineKeyboardButton("✅ Done", callback_data="done")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🎥 Pick your genres to unlock movie magic! 🍿",
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
                f"Selected: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '') for g in context.user_data['selected_genres']])}\nConfirm? ✅",
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
            if (i + 1) % 3 == 0 or i == len(GENRES) - 1:
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
            f"🎉 Locked in: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '') for g in context.user_data['selected_genres']])}\n"
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
            stats_message += f"{GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '')}: {count} users\n"
        
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

async def check_release(update, context):
    try:
        if not context.args:
            await update.message.reply_text("🔍 Provide a movie/series name: /checkrelease <name>")
            return
        
        query = ' '.join(context.args)
        results = search_api.multi(query)
        
        if not results:
            await update.message.reply_text(f"😔 No results for '{query}'.")
            return
        
        result = results[0]
        media_type = result.media_type
        details = movie_api.details(result.id) if media_type == 'movie' else tv_api.details(result.id)
        
        cast = ', '.join([c['name'] for c in details.credits.cast[:3]]) if details.credits.cast else 'N/A'
        director = next((c['name'] for c in details.credits.crew if c['job'] == 'Director'), 'N/A')
        languages = ', '.join([l['english_name'] for l in details.spoken_languages]) if details.spoken_languages else 'N/A'
        
        share_button = [[InlineKeyboardButton("📤 Share", switch_inline_query=f"Check out {result.title or result.name}!")]]
        reply_markup = InlineKeyboardMarkup(share_button)
        
        await update.message.reply_photo(
            photo=f"https://image.tmdb.org/t/p/w500{result.poster_path}" if result.poster_path else "https://via.placeholder.com/500",
            caption=(
                f"🎥 **{result.title or result.name}**\n"
                f"Type: {'Movie' if media_type == 'movie' else 'Series'}\n"
                f"Release: {result.release_date or result.first_air_date or 'TBA'}\n"
                f"Overview: {result.overview[:200]}...\n"
                f"Cast: {cast}\n"
                f"Director: {director}\n"
                f"Languages: {languages}"
            ),
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error in check_release: {e}")
        await update.message.reply_text("🚨 Error fetching details.")

def fetch_upcoming_movies(genres):
    try:
        movies = []
        for genre in genres:
            if genre == 'bollywood':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'hi':
                        movies.append(fetch_movie_details(m, genre))
            elif genre == 'hollywood':
                tmdb_movies = movie_api.upcoming(region='US')
                for m in tmdb_movies:
                    if m.original_language == 'en':
                        movies.append(fetch_movie_details(m, genre))
            elif genre == 'tamil':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'ta':
                        movies.append(fetch_movie_details(m, genre))
            elif genre == 'tollywood':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'te':
                        movies.append(fetch_movie_details(m, genre))
            elif genre == 'gujarati':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'gu':
                        movies.append(fetch_movie_details(m, genre))
            elif genre == 'marathi':
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'mr':
                        movies.append(fetch_movie_details(m, genre))
            elif genre == 'korean':
                tmdb_shows = tv_api.on_the_air(language='ko')
                for s in tmdb_shows:
                    if s.original_language == 'ko':
                        movies.append(fetch_tv_details(s, genre))
            elif genre == 'indian':
                tmdb_shows = tv_api.on_the_air(region='IN')
                for s in tmdb_shows:
                    if s.origin_country and 'IN' in s.origin_country:
                        movies.append(fetch_tv_details(s, genre))
            elif genre == 'webseries':
                tmdb_shows = tv_api.popular()
                for s in tmdb_shows:
                    movies.append(fetch_tv_details(s, genre))
        
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
        details = movie_api.details(movie.id)
        cast = ', '.join([c['name'] for c in details.credits.cast[:3]]) if details.credits.cast else 'N/A'
        director = next((c['name'] for c in details.credits.crew if c['job'] == 'Director'), 'N/A')
        languages = ', '.join([l['english_name'] for l in details.spoken_languages]) if details.spoken_languages else 'N/A'
        return {
            'title': movie.title,
            'genre': GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', ''),
            'release_date': movie.release_date or 'TBA',
            'overview': movie.overview,
            'poster': f"https://image.tmdb.org/t/p/w500{movie.poster_path}" if movie.poster_path else "https://via.placeholder.com/500",
            'cast': cast,
            'director': director,
            'languages': languages
        }
    except Exception as e:
        logger.error(f"Error fetching movie details: {e}")
        return {}

def fetch_tv_details(show, genre):
    try:
        details = tv_api.details(show.id)
        cast = ', '.join([c['name'] for c in details.credits.cast[:3]]) if details.credits.cast else 'N/A'
        director = next((c['name'] for c in details.credits.crew if c['job'] == 'Director'), 'N/A')
        languages = ', '.join([l['english_name'] for l in details.spoken_languages]) if details.spoken_languages else 'N/A'
        return {
            'title': show.name,
            'genre': GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', ''),
            'release_date': show.first_air_date or 'TBA',
            'overview': show.overview,
            'poster': f"https://image.tmdb.org/t/p/w500{show.poster_path}" if show.poster_path else "https://via.placeholder.com/500",
            'cast': cast,
            'director': director,
            'languages': languages
        }
    except Exception as e:
        logger.error(f"Error fetching TV details: {e}")
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
            entry_points=[CommandHandler('start', start)],
            states={
                GENRE: [CallbackQueryHandler(genre_selection)],
                CONFIRM: [CallbackQueryHandler(confirm_selection)]
            },
            fallbacks=[],
            per_chat=True,
            per_user=True
        )
        
        app.add_handler(conv_handler)
        app.add_handler(CommandHandler('upcoming', upcoming))
        app.add_handler(CommandHandler('reset', reset))
        app.add_handler(CommandHandler('stats', stats))
        app.add_handler(CommandHandler('broadcast', broadcast))
        app.add_handler(CommandHandler('checkrelease', check_release))
        app.add_error_handler(error_handler)
        
        # Schedule notifications (daily for no genres, monthly/weekly for releases, news)
        app.job_queue.run_repeating(notify_users, interval=86400, first=10)
        
        app.run_polling()
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == '__main__':
    main()
