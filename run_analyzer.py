from biliob_analyzer.author_analyzer import AuthorAnalyzer
from biliob_analyzer.video_analyzer import VideoAnalyzer
# import biliob_analyzer.author_rate_caculate
import biliob_analyzer.author_fans_watcher
# import biliob_analyzer.author_rank
# import biliob_analyzer.video_rank
from biliob_analyzer.add_keyword import KeywordAdder

kwAdder = KeywordAdder()
kwAdder.add_all_author()
kwAdder.add_all_video()
# kwAdder.add_omitted()

author_analyzer = AuthorAnalyzer()
video_analyzer = VideoAnalyzer()

author_analyzer.author_filter()
video_analyzer.video_filter()
