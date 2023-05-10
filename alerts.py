# -*- coding: UTF-8 -*-
'''
@Project : 
@File : alerts.py
@Author : zhangziting
@Date : 2023/4/23 10:08
'''

import requests
import json


## 替换为你的自定义机器人的webhook地址。


def send_card(url: str, content: dict):
    card = json.dumps(content)
    body =json.dumps({"msg_type": "interactive","card":card})
    headers = {"Content-Type":"application/json"}
    requests.post(url=url, data=body, headers=headers)

def info(info,timestr):

    content ={
          "elements": [
            {
              "tag": "markdown",
              # "content": f"<font  color=\"red\">**{taskmode}!!!**</font>\n\n{info}\n\n{timestr}\n\n<at id=all></at>"
              "content": f"\n{info}\n\n{timestr}\n\n"
            },
            {
              "tag": "hr"
            }
          ],
          "header": {
            "template": "red",
            "title": {
              "content": "MYSQL增量定时归档失败",
              "tag": "plain_text"
            }
          },
          "card_link": {
            "url": "",
            "pc_url": "",
            "android_url": "",
            "ios_url": ""
          }
        }
    ## 替换为你的自定义机器人的webhook地址。
    url = ""
    return send_card(url, content)



