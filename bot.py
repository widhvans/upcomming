import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler, MessageHandler, filters
import pymongo
from tmdbv3api import TMDb, Movie, TV
import requests
from datetime import datetime, timedelta
import asyncio
import config

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

# Available genres
GENRES = {
    'bollywood': 'Bollywood',
    'hollywood': 'Hollywood',
    'tamil': 'Tamil',
    'tollywood': 'Tollywood',
    'gujarati': 'Gujarati',
    'marathi': 'Marathi',
    'korean': 'Korean Dramas',
    'indian': 'Indian Dramas',
    'webseries': 'Web Series'
}

async def start(update, context):
    user_id = update.effective_user.id
    user = users_collection.find_one({'user_id': user_id})
    
    if user:
        await update.message.reply_text(
            f"Welcome back! Your preferences: {', '.join(user['genres'])}\n"
            "Use /upcoming to see upcoming releases or /reset to change preferences."
        )
        return ConversationHandler.END
    
    keyboard = [
        [InlineKeyboardButton(GENRES[genre], callback_data=genre)] for genre in GENRES
    ]
    keyboard.append([InlineKeyboardButton("Done", callback_data="done")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Welcome to the Movie Bot! Select your favorite genres (multiple allowed):",
        reply_markup=reply_markup
    )
    context.user_data['selected_genres'] = []
    return GENRE

async def genre_selection(update, context):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "done":
        if not context.user_data['selected_genres']:
            await query.message.reply_text("Please select at least one genre!")
            return GENRE
        keyboard = [
            [InlineKeyboardButton("Confirm", callback_data="confirm")],
            [InlineKeyboardButton("Cancel", callback_data="cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            f"Selected genres: {', '.join([GENRES[g] for g in context.user_data['selected_genres']])}\nConfirm?",
            reply_markup=reply_markup
        )
        return CONFIRM
    
    if data in context.user_data['selected_genres']:
        context.user_data['selected_genres'].remove(data)
    else:
        context.user_data['selected_genres'].append(data)
    
    keyboard = [
        [InlineKeyboardButton(
            f"{GENRES[genre]} {'✅' if genre in context.user_data['selected_genres'] else ''}",
            callback_data=genre
        )] for genre in GENRES
    ]
    keyboard.append([InlineKeyboardButton("Done", callback_data="done")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.edit_reply_markup(reply_markup=reply_markup)
    return GENRE

async def confirm_selection(update, context):
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "cancel":
        context.user_data['selected_genres'] = []
        await query.message.reply_text("Selection cancelled. Use /start to try again.")
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    users_collection.insert_one({
        'user_id': user_id,
        'genres': context.user_data['selected_genres'],
        'created_at': datetime.utcnow()
    })
    
    await query.message.reply_text(
        f"Preferences saved: {', '.join([GENRES[g] for g in context.user_data['selected_genres']])}\n"
        "You'll get notifications for upcoming releases! Use /upcoming to see them now."
    )
    context.user_data['selected_genres'] = []
    return ConversationHandler.END

async def reset(update, context):
    user_id = update.effective_user.id
    users_collection.delete_one({'user_id': user_id})
    await update.message.reply_text("Preferences reset. Use /start to set new preferences.")
    return ConversationHandler.END

async def upcoming(update, context):
    user_id = update.effective_user.id
    user = users_collection.find_one({'user_id': user_id})
    
    if not user:
        await update.message.reply_text("Please set your preferences using /start.")
        return
    
    genres = user['genres']
    movies = fetch_upcoming_movies(genres)
    
    if not movies:
        await update.message.reply_text("No upcoming releases found for your preferences.")
        return
    
    for movie in movies[:5]:  # Limit to 5 for brevity
        await update.message.reply_photo(
            photo=movie['poster'],
            caption=(
                f"**{movie['title']}**\n"
                f"Genre: {movie['genre']}\n"
                f"Release: {movie['release_date']}\n"
                f"Overview: {movie['overview'][:200]}...\n"
                f"Context: {movie['context']}"
            )
        )

def fetch_upcoming_movies(genres):
    # Simplified mapping of bot genres to TMDb genres
    genre_mapping = {
        'bollywood': 18,  # Drama (common for Bollywood)
        'hollywood': 28,  # Action (common for Hollywood)
        'tamil': 18,
        'tollywood': 18,
        'gujarati': 18,
        'marathi': 18,
        'korean': 10764,  # Reality (proxy for Korean dramas)
        'indian': 18,
        'webseries': 10765  # Sci-Fi & Fantasy (proxy for web series)
    }
    
    movies = []
    for genre in genres:
        # Fetch movies
        tmdb_movies = movie_api.upcoming()
        for m in tmdb_movies:
            if genre_mapping.get(genre) in m.genre_ids:
                movies.append({
                    'title': m.title,
                    'genre': GENRES[genre],
                    'release_date': m.release_date,
                    'overview': m.overview,
                    'poster': f"https://image.tmdb.org/t/p/w500{m.poster_path}" if m.poster_path else "https://via.placeholder.com/500",
                    'context': "Fetched from TMDb"
                })
        
        # Fetch TV shows (for web series, Korean/Indian dramas)
        tmdb_shows = tv_api.upcoming()
        for s in tmdb_shows:
            if genre_mapping.get(genre) in s.genre_ids:
                movies.append({
                    'title': s.name,
                    'genre': GENRES[genre],
                    'release_date': s.first_air_date,
                    'overview': s.overview,
                    'poster': f"https://image.tmdb.org/t/p/w500{s.poster_path}" if s.poster_path else "https://via.placeholder.com/500",
                    'context': "Fetched from TMDb (TV)"
                })
    
    # Store in MongoDB
    for movie in movies:
        movies_collection.update_one(
            {'title': movie['title'], 'release_date': movie['release_date']},
            {'$set': movie},
            upsert=True
        )
    
    return movies

async def notify_users(context):
    upcoming_movies = movies_collection.find({
        'release_date': {
            '$gte': datetime.utcnow().strftime('%Y-%m-%d'),
            '$lte': (datetime.utcnow() + timedelta(days=7)).strftime('%Y-%m-%d')
        }
    })
    
    for movie in upcoming_movies:
        for user in users_collection.find({'genres': movie['genre'].lower()}):
            await context.bot.send_photo(
                chat_id=user['user_id'],
                photo=movie['poster'],
                caption=(
                    f"Upcoming: **{movie['title']}**\n"
                    f"Genre: {movie['genre']}\n"
                    f"Release: {movie['release_date']}\n"
                    f"Overview: {movie['overview'][:200]}..."
                )
            )

def main():
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
    
    # Schedule notifications
    app.job_queue.run_repeating(notify_users, interval=86400, first=10)
    
    app.run_polling()

if __name__ == '__main__':
    main()
