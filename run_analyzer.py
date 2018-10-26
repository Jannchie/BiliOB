from biliob_analyzer.author_analyzer import AuthorAnalyzer
from biliob_analyzer.video_analyzer import VideoAnalyzer

author_analyzer = AuthorAnalyzer()
video_analyzer = VideoAnalyzer()

author_analyzer.author_filter()
video_analyzer.video_filter()