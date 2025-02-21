#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import urllib.parse

import asyncio
import aiofiles
import aiofiles.os
import aiohttp

from lxml import html
from contextvars import ContextVar

from progress import percent_complete

RequestFrequency = 5 # seconds / request
BaseURL = "https://learn.lianglianglee.com"
BaseHeaders = {
    "user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}
ObsidianVault = "~/ObsidianVault/lianglianglee"

completed_task_count = ContextVar("completed_task_count", default=0)
total_task_count = ContextVar("total_task_count", default=0)


logger = logging.getLogger("spider")

async def create_dir_if_not_exists(path):
    abs_path = os.path.expanduser(path)
    if not await aiofiles.os.path.exists(abs_path):
        await aiofiles.os.makedirs(abs_path)


async def fetch_html(session, url):
    # avoid too many requests
    await asyncio.sleep(RequestFrequency)

    async with session.get(url) as resp:
        if resp.status != 200:
            raise Exception(f"fetch {url} failed, status: {resp.status}")
        return await resp.text()


async def dl_file(session, url, output):
    await create_dir_if_not_exists(os.path.dirname(output))

    await asyncio.sleep(RequestFrequency)
    async with session.get(url) as resp:
        async with aiofiles.open(output, 'wb') as f:
            await f.write(await resp.read())


def parse_toc(html_content):
    results = []

    doc = html.fromstring(html_content)
    posts = doc.body.find_class('book-post')
    if not posts:
        return []

    toc = posts[0].find('.//ul')
    for item in toc.iterchildren():
        tag_a = item.find('.//a')
        if tag_a is not None:
            item = {"type":"scrape", "title": tag_a.text, "href": tag_a.get('href')}
            results.append(item)
    return results


async def get_root_toc(session, url_path):
    html = await fetch_html(session, url_path)
    return parse_toc(html)


async def get_sub_toc(queue, session, item):
    url_path = item["href"]
    html = await fetch_html(session, url_path)
    sub_toc_list = parse_toc(html)
    for sub_toc in sub_toc_list:
        sub_toc["column"] = item["title"]
        await queue.put(sub_toc)
        total_task_count.set(total_task_count.get() + 1)


async def load_content_from_local(file_path):
    async with aiofiles.open(file_path, 'r') as f:
        data = await f.read()
        data = f"<div class='book-post'><div>{data}</div></div>"
        return data


async def parse_imgs(content, queue, url_path, abs_parent_dir):
    imgs = content.findall('.//img')
    for img in imgs:
        # make sure src is quoted
        relative_path = img.get('src')
        if relative_path == urllib.parse.unquote(relative_path):
            relative_path = urllib.parse.quote(relative_path, safe='/')

        # if file has no extension, add .png and update src
        if len(img.get('src').split('.')) < 2:
            img.set('src', f"{img.get('src')}.png")

        download_url = f"{os.path.dirname(url_path)}/{relative_path}"
        output = os.path.join(abs_parent_dir, img.get('src'))
        item = {"type":"dl_img", "download_url": download_url, "output": output}
        await queue.put(item)
        total_task_count.set(total_task_count.get() + 1)


async def scrape_and_persist(queue, session, item):
    logger.debug("scrape_and_persist: %s-%s", item['column'], item['title'])
    url_path = item["href"]
    # make sure url_path is quoted
    unquote_url_path = urllib.parse.unquote(url_path)
    if unquote_url_path == url_path:
        url_path = urllib.parse.quote(url_path, safe='/')

    dl_file_name = os.path.basename(item["href"])
    abs_parent_dir = os.path.join(ObsidianVault, item["column"])
    dl_file_abs_path = os.path.join(abs_parent_dir, dl_file_name)

    # logger.debug("dl_file_abs_path: %s", dl_file_abs_path)
    file_exists = await aiofiles.os.path.exists(dl_file_abs_path)
    if file_exists:
        logger.info(f"file {dl_file_abs_path} already exists. load from local")
        data = await load_content_from_local(dl_file_abs_path)
    else:
        data = await fetch_html(session, url_path)

    doc = html.fromstring(data)
    posts = doc.body.find_class('book-post')

    # if no posts element found, save the whole html
    if not posts and not file_exists:
        async with aiofiles.open(dl_file_abs_path, 'w') as f:
            await f.write(data)
        return

    post = posts[0]
    content = post.find('.//div[p]')
    await parse_imgs(content, queue, url_path, abs_parent_dir)

    if not file_exists:
        await create_dir_if_not_exists(abs_parent_dir)
        async with aiofiles.open(dl_file_abs_path, 'w') as f:
            data = ['\n']
            for item in content.iterchildren():
                data.append(html.tostring(item, encoding="utf-8").decode())
            await f.write(''.join(data))


async def dl_img(session, item):
    download = await aiofiles.os.path.exists(item["output"])
    if download:
        logger.info(f"image {item['output']} already exists")
        return
    await dl_file(session, item["download_url"], item["output"])


async def scrape_worker(queue, session):
    while not queue.empty():
        try:
            item = await queue.get()
            if item["type"] == "scrape":
                await scrape_and_persist(queue, session, item)
            else:
                await dl_img(session, item)
            completed_task_count.set(completed_task_count.get() + 1)
        except Exception as e:
            logger.error(f"scrape {item['href']} failed, error: {e}", exc_info=True, stack_info=True)


async def progress_bar(queue, item_title):
    total_count = total_task_count.get()
    completed_count = completed_task_count.get()
    while not queue.empty():
        await asyncio.sleep(5)
        total_count = total_task_count.get()
        completed_count = completed_task_count.get()
        title = f"{item_title}, total: {total_count}, completed: {completed_count}"
        percent_complete(completed_count, total_count, title=title)
    percent_complete(total_count, total_count, title=title)


async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    queue = asyncio.Queue()

    completed_task_count.set(0)
    total_task_count.set(0)

    context = asyncio.current_task().get_context()

    async with aiohttp.ClientSession(base_url=BaseURL, headers=BaseHeaders) as session:
        logger.info(f"start scraping {BaseURL}")
        root_toc = await get_root_toc(session, "/")
        logger.info("found %d columns", len(root_toc))
        for item in root_toc[11:]:
            # reset progress
            completed_task_count.set(0)
            total_task_count.set(0)

            logger.info(f"start scraping: {item['title']}")
            await get_sub_toc(queue, session, item)

            progress = asyncio.create_task(progress_bar(queue, item["title"]), context=context)
            worker = asyncio.create_task(scrape_worker(queue, session), context=context)
            await worker
            await progress

if __name__ == "__main__":
    asyncio.run(main())
