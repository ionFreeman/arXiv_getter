# from https://stackoverflow.com/a/41915132/781641
def jupyter_upload(token, filePath, resourceDstPath, jupyterUrl='http://localhost:8888'):
    """
        Uploads File to Jupyter Notebook Server
        ----------------------------------------
        :param token:
            The authorization token issued by Jupyter for authentification
            (enabled by default as of version 4.3.0)
        :param filePath:
            The file path to the local content to be uploaded

        :param resourceDstPath:
            The path where resource should be placed.
            The destination directory must exist.

        :param jupyterUrl:
            The url to the jupyter server. Default value is typical localhost installation.

        :return: server response

    """
    import os
    import base64
    import urllib
    import json
    import requests
    dstPath = urllib.quote(resourceDstPath)
    dstUrl = '%s/api/contents/%s' % (jupyterUrl, dstPath)
    fileName = filePath[1 + filePath.rfind(os.sep):]
    headers = {}
    headers['Authorization'] = 'token '+token
    with open(filePath, 'r') as myfile:
        data=myfile.read()
        b64data=base64.encodestring(data)
        body = json.dumps({
            'content':b64data,
            'name': fileName,
            'path': resourceDstPath,
            'format': 'base64',
            'type':'file'
        })
        return requests.put(dstUrl, data=body, headers=headers, verify=True)