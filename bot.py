import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler, MessageHandler, filters
import pymongo
from tmdbv3api import TMDb, Movie, TV
import requests
from datetime import datetime, timedelta, UTC
import asyncio
import config
import logging

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

# MongoDB setup
client = pymongo.MongoClient(config.MONGO_URI)
db = client['movie_bot']
users_collection = db['users']
movies_collection = db['movies']

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
                f"🎉 Welcome back! Your preferences: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '') for g in user['genres']])}\n"
                "Use /upcoming to see releases or /reset to change preferences. 🍿"
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
            "🎥 Welcome to Movie Mastermind! Select your favorite genres (multiple allowed):",
            reply_markup=reply_markup
        )
        context.user_data['selected_genres'] = []
        return GENRE
    except Exception as e:
        logger.error(f"Error in start: {e}")
        await update.message.reply_text("🚨 An error occurred. Please try again.")
        return ConversationHandler.END

async def genre_selection(update, context):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "done":
            if not context.user_data['selected_genres']:
                await query.message.reply_text("⚠️ Please select at least one genre!")
                return GENRE
            keyboard = [
                [InlineKeyboardButton("✔️ Confirm", callback_data="confirm")],
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(
                f"Selected genres: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '') for g in context.user_data['selected_genres']])}\nConfirm? ✅",
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
        await query.message.reply_text("🚨 An error occurred. Please try again.")
        return GENRE

async def confirm_selection(update, context):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "cancel":
            context.user_data['selected_genres'] = []
            await query.message.reply_text("Selection cancelled. Use /start to try again. 🎬")
            return ConversationHandler.END
        
        user_id = update.effective_user.id
        users_collection.insert_one({
            'user_id': user_id,
            'genres': context.user_data['selected_genres'],
            'created_at': datetime.now(UTC)
        })
        
        await query.message.reply_text(
            f"🎉 Preferences saved: {', '.join([GENRES[g].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '') for g in context.user_data['selected_genres']])}\n"
            "You'll get notifications for upcoming releases! Use /upcoming to see them now. 🍿"
        )
        context.user_data['selected_genres'] = []
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in confirm_selection: {e}")
        await query.message.reply_text("🚨 An error occurred. Please try again.")
        return ConversationHandler.END

async def reset(update, context):
    try:
        user_id = update.effective_user.id
        users_collection.delete_one({'user_id': user_id})
        await update.message.reply_text("🔄 Preferences reset. Use /start to set new preferences.")
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in reset: {e}")
        await update.message.reply_text("🚨 An error occurred. Please try again.")
        return ConversationHandler.END

async def stats(update, context):
    try:
        user_id = update.effective_user.id
        if str(user_id) != config.OWNER_ID:
            await update.message.reply_text("🔒 This command is for admins only.")
            return
        
        total_users = users_collection.count_documents({})
        genres_count = {}
        for user in users_collection.find():
            for genre in user['genres']:
                genres_count[genre] = genres_count.get(genre, 0) + 1
        
        stats_message = f"📊 Bot Stats\n\nTotal Users: {total_users}\n\nGenres:\n"
        for genre, count in genres_count.items():
            stats_message += f"{GENRES[genre].replace('🎬 ', '').replace('📺 ', '').replace('🌐 ', '')}: {count} users\n"
        
        await update.message.reply_text(stats_message)
    except Exception as e:
        logger.error(f"Error in stats: {e}")
        await update.message.reply_text("🚨 An error occurred. Please try again.")

async def upcoming(update, context):
    try:
        user_id = update.effective_user.id
        user = users_collection.find_one({'user_id': user_id})
        
        if not user:
            await update.message.reply_text("⚠️ Please set your preferences using /start.")
            return
        
        genres = user['genres']
        movies = fetch_upcoming_movies(genres)
        
        if not movies:
            await update.message.reply_text("😔 No upcoming releases found for your preferences.")
            return
        
        for movie in movies[:5]:  # Limit to 5 for brevity
            await update.message.reply_photo(
                photo=movie['poster'],
                caption=(
                    f"🎥 **{movie['title']}**\n"
                    f"Genre: {movie['genre']}\n"
                    f"Release: {movie['release_date']}\n"
                    f"Overview: {movie['overview'][:200]}...\n"
                    f"Source: {movie['context']}"
                )
            )
    except Exception as e:
        logger.error(f"Error in upcoming: {e}")
        await update.message.reply_text("🚨 An error occurred while fetching upcoming releases.")
        return

def fetch_upcoming_movies(genres):
    try:
        movies = []
        for genre in genres:
            if genre == 'bollywood':
                # Bollywood: Indian movies, Hindi language
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'hi':
                        movies.append({
                            'title': m.title,
                            'genre': GENRES[genre].replace('🎬 ', ''),
                            'release_date': m.release_date,
                            'overview': m.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Bollywood)"
                        })
            elif genre == 'hollywood':
                # Hollywood: English, US region
                tmdb_movies = movie_api.upcoming(region='US')
                for m in tmdb_movies:
                    if m.original_language == 'en':
                        movies.append({
                            'title': m.title,
                            'genre': GENRES[genre].replace('🎬 ', ''),
                            'release_date': m.release_date,
                            'overview': m.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Hollywood)"
                        })
            elif genre == 'tamil':
                # Tamil: Tamil language
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'ta':
                        movies.append({
                            'title': m.title,
                            'genre': GENRES[genre].replace('🎬 ', ''),
                            'release_date': m.release_date,
                            'overview': m.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Tamil)"
                        })
            elif genre == 'tollywood':
                # Tollywood: Telugu language
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'te':
                        movies.append({
                            'title': m.title,
                            'genre': GENRES[genre].replace('🎬 ', ''),
                            'release_date': m.release_date,
                            'overview': m.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Tollywood)"
                        })
            elif genre == 'gujarati':
                # Gujarati: Gujarati language
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'gu':
                        movies.append({
                            'title': m.title,
                            'genre': GENRES[genre].replace('🎬 ', ''),
                            'release_date': m.release_date,
                            'overview': m.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Gujarati)"
                        })
            elif genre == 'marathi':
                # Marathi: Marathi language
                tmdb_movies = movie_api.upcoming(region='IN')
                for m in tmdb_movies:
                    if m.original_language == 'mr':
                        movies.append({
                            'title': m.title,
                            'genre': GENRES[genre].replace('🎬 ', ''),
                            'release_date': m.release_date,
                            'overview': m.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Marathi)"
                        })
            elif genre == 'korean':
                # Korean Dramas: Korean TV shows
                tmdb_shows = tv_api.on_the_air(language='ko')
                for s in tmdb_shows:
                    if s.original_language == 'ko':
                        movies.append({
                            'title': s.name,
                            'genre': GENRES[genre].replace('📺 ', ''),
                            'release_date': s.first_air_date,
                            'overview': s.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{s.poster_path}" if s.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Korean Dramas)"
                        })
            elif genre == 'indian':
                # Indian Dramas: Indian TV shows
                tmdb_shows = tv_api.on_the_air(region='IN')
                for s in tmdb_shows:
                    if s.origin_country and 'IN' in s.origin_country:
                        movies.append({
                            'title': s.name,
                            'genre': GENRES[genre].replace('📺 ', ''),
                            'release_date': s.first_air_date,
                            'overview': s.overview,
                            'poster': f"https://image.tmdb.org/t/p/w500{s.poster_path}" if s.poster_path else "https://via.placeholder.com/500",
                            'context': "TMDb (Indian Dramas)"
                        })
            elif genre == 'webseries':
                # Web Series: Popular TV shows
                tmdb_shows = tv_api.popular()
                for s in tmdb_shows:
                    movies.append({
                        'title': s.name,
                        'genre': GENRES[genre].replace('🌐 ', ''),
                        'release_date': s.first_air_date,
                        'overview': s.overview,
                        'poster': f"https://image.tmdb.org/t/p/w500{s.poster_path}" if s.poster_path else "https://via.placeholder.com/500",
                        'context': "TMDb (Web Series)"
                    })
        
        # Remove duplicates by title and release_date
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

async def notify_users(context):
    try:
        upcoming_movies = movies_collection.find({
            'release_date': {
                '$gte': datetime.now(UTC).strftime('%Y-%m-%d'),
                '$lte': (datetime.now(UTC) + timedelta(days=7)).strftime('%Y-%m-%d')
            }
        })
        
        for movie in upcoming_movies:
            for user in users_collection.find({'genres': movie['genre'].lower()}):
                await context.bot.send_photo(
                    chat_id=user['user_id'],
                    photo=movie['poster'],
                    caption=(
                        f"📢 Upcoming: **{movie['title']}**\n"
                        f"Genre: {movie['genre']}\n"
                        f"Release: {movie['release_date']}\n"
                        f"Overview: {movie['overview'][:200]}..."
                    )
                )
    except Exception as e:
        logger.error(f"Error in notify_users: {e}")

async def error_handler(update, context):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text("🚨 An error occurred. Please try again later.")

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
        app.add_error_handler(error_handler)
        
        # Schedule notifications
        app.job_queue.run_repeating(notify_users, interval=86400, first=10)
        
        app.run_polling()
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == '__main__':
    main()
