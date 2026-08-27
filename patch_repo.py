import os
import subprocess

def run(cmd):
    subprocess.run(cmd, shell=True, check=True)

def patch_files(is_heroku=False):
    # LICENSE
    with open('LICENSE', 'r') as f:
        c = f.read()
    c = c.replace('Copyright (c) 2026', 'Copyright (c) 2026 fiksofficial')
    with open('LICENSE', 'w') as f:
        f.write(c)

    # pyproject.toml
    with open('pyproject.toml', 'r') as f:
        c = f.read()
    
    if 'authors =' not in c:
        c = c.replace('version = "0.1.0"', 'version = "0.1.0"\nauthors = [{name = "fiksofficial"}]')
    
    c = c.replace('github.com/user/telethon-webproxy', 'github.com/fiksofficial/telethon-webproxy')
    c = c.replace('github.com/user/herokutl-webproxy', 'github.com/fiksofficial/telethon-webproxy')
    with open('pyproject.toml', 'w') as f:
        f.write(c)

    # README.md
    with open('README.md', 'r') as f:
        c = f.read()
    if is_heroku:
        c = c.replace('telethon-webproxy', 'herokutl-webproxy')
        c = c.replace('telethon_webproxy', 'herokutl_webproxy')
        c = c.replace('telethon-v1', 'herokutl-v1')
        c = c.replace('telethon-v2', 'herokutl-v2')
    with open('README.md', 'w') as f:
        f.write(c)

# Patch forheroku branch
run('git checkout forheroku')
patch_files(is_heroku=True)
run('git add . && git commit -m "Update author, URLs, and README"')
run('git push origin forheroku')

# Patch master branch
run('git checkout master')
patch_files(is_heroku=False)
run('git add . && git commit -m "Update author and URLs"')
run('git push origin master')

