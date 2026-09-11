`update.html` is bundled into `BiliPDJ-Web-Updater.exe` and served by its temporary loopback HTTP server during Web Portable updates.

It must remain self-contained: no CDN, external JavaScript, fonts, images, or main-server resources are required. This allows the page, Tetris game, and progress polling to remain available while the main Web backend and launcher are stopped and replaced.
