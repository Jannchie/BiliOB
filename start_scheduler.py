from biliob_scheduler.scheduler import auto_crawl_bangumi
from biliob_scheduler.scheduler import auto_add_video
from biliob_scheduler.scheduler import auto_add_author
from biliob_scheduler.scheduler import crawlOnlineTopListData
from biliob_scheduler.scheduler import update_author
from biliob_scheduler.scheduler import update_unfocus_video
from biliob_scheduler.scheduler import update_video
from biliob_scheduler.scheduler import gen_online
auto_crawl_bangumi()
auto_add_video()
auto_add_author()
crawlOnlineTopListData()
update_author()
update_unfocus_video()
update_video()
gen_online()
