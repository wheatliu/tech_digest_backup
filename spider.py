#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import hashlib
import logging
import urllib.parse

import asyncio
import aiofiles
import aiofiles.os
import aiohttp

from lxml import html
from contextvars import ContextVar

from markitdown import MarkItDown

from progress import percent_complete

RequestFrequency = 5 # seconds / request
BaseURL = "https://learn.lianglianglee.com"
BaseHeaders = {
    "user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}
ObsidianVaultPath = "/Users/wheat/WorkSpace/notes/CS/TechDigest"
Workspace = "/Users/wheat/WorkSpace/tmp"

completed_task_count = ContextVar("completed_task_count", default=0)
total_task_count = ContextVar("total_task_count", default=0)

logger = logging.getLogger("spider")

md = MarkItDown()

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

    file_name = os.path.basename(item["href"])
    raw_file_name = f"{hashlib.md5(file_name.encode()).hexdigest()}.html"

    column_dir = os.path.join(ObsidianVaultPath, item["column"])
    raw_column_dir = os.path.join(Workspace, item["column"])

    md_file_path = os.path.join(column_dir, file_name)
    raw_file_path = os.path.join(raw_column_dir, raw_file_name)

    downloaded = await aiofiles.os.path.exists(raw_file_path)
    processed = await aiofiles.os.path.exists(md_file_path)
    if downloaded:
        logger.info(f"file {raw_file_path} already exists. load from local")
        data = await load_content_from_local(raw_file_path)
    else:
        data = await fetch_html(session, url_path)

    doc = html.fromstring(data)
    posts = doc.body.find_class('book-post')

    # if no posts element found, save the whole html
    if not posts and not downloaded:
        async with aiofiles.open(md_file_path, 'w') as f:
            await f.write(data)
        return

    post = posts[0]
    content = post.find('.//div[p]')
    # title = post.find('.//h1')
    await parse_imgs(content, queue, url_path, column_dir)

    if not downloaded:
        await create_dir_if_not_exists(raw_column_dir)
        async with aiofiles.open(raw_file_path, 'w') as f:
            data = ['\n']
            for item in content.iterchildren():
                data.append(html.tostring(item, encoding="utf-8").decode())
            await f.write(''.join(data))

    if not processed:
        await create_dir_if_not_exists(column_dir)
        async with aiofiles.open(md_file_path, 'w') as f:
            result = md.convert_local(raw_file_path)
            await f.write(result.text_content)


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
        await asyncio.sleep(RequestFrequency)
        total_count = total_task_count.get()
        completed_count = completed_task_count.get()
        title = f"{item_title}, total: {total_count}, completed: {completed_count}"
        percent_complete(completed_count, total_count, title=title)
    await asyncio.sleep(RequestFrequency)
    total_count = total_task_count.get()
    percent_complete(total_count, total_count, title=title)


def generate_toc(root_toc, args):
    logger.info("args: %s", args)
    if args.all:
        logger.info("scrape all columns")
        return root_toc
    elif args.columns:
        toc = [item for item in root_toc if item["title"] in args.columns]
        logger.info("scrape specific columns: %s", '\n'.join([item["title"] for item in toc]))
        return toc
    elif args.range:
        start, end = args.range.split('-')
        toc = root_toc[int(start)-1:int(end)]
        logger.info("scrape specific range of columns: %s", '\n'.join([item["title"] for item in toc]))
        return toc
    elif args.keyword:
        toc = [item for item in root_toc if args.keyword in item["title"]]
        logger.info("scrape columns with keyword: %s", '\n'.join([item["title"] for item in toc]))
        return toc
    else:
        return []


async def main():
    global Workspace
    global ObsidianVaultPath

    parser = argparse.ArgumentParser(prog='spider', description='Scrape content from learn.lianglianglee.com')
    parser.add_argument('-d', '--debug', action='store_true', help='debug mode')
    parser.add_argument('-o', '--output', type=str, help='output path', default=ObsidianVaultPath, required=True)
    parser.add_argument('-w', '--workspace', type=str, help='workspace path', default=Workspace, required=True)

    mutex_group = parser.add_mutually_exclusive_group(required=True)
    mutex_group.add_argument('-a', '--all', action='store_true', help='scrape all columns', default=False)
    mutex_group.add_argument('-c', '--column',action='append', dest='columns', help='scrape specific column, e.g. "24讲吃透分布式数据库-完"')
    mutex_group.add_argument('-r', '--range', type=str, help='scrape specific range of columns, e.g. 1-3')
    mutex_group.add_argument('-k', '--keyword', type=str, help='scrape columns with keyword, e.g. "分布式"')
    args = parser.parse_args()

    
    ObsidianVaultPath = args.output
    Workspace = args.workspace

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    queue = asyncio.Queue()

    completed_task_count.set(0)
    total_task_count.set(0)

    context = asyncio.current_task().get_context()

    # disable ssl verification
    connector = aiohttp.TCPConnector(keepalive_timeout=60, ssl=False)
    async with aiohttp.ClientSession(base_url=BaseURL, headers=BaseHeaders, connector=connector) as session:
        logger.info(f"\nstart scraping {BaseURL}")
        root_toc = await get_root_toc(session, "/")
        toc = generate_toc(root_toc, args)
        for item in toc:
            # reset progress
            completed_task_count.set(0)
            total_task_count.set(0)

            logger.info(f"start scraping: {item['title']}")
            await get_sub_toc(queue, session, item)

            progress = asyncio.create_task(progress_bar(queue, item["title"]), context=context)
            worker = asyncio.create_task(scrape_worker(queue, session), context=context)
            await worker
            await progress
    
    # Graceful Shutdown
    # To avoid "ResourceWarning: unclosed transport" warning
    # see https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    await asyncio.sleep(2)    

if __name__ == "__main__":
    asyncio.run(main())

