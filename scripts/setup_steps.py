"""導入案内の段と画面文言の定義。表示処理は持たない。

端末の案内（scripts/setup_wizard.py）が読む。将来ブラウザの初回案内も同じ定義を読む。
文言は専門語ではなく「何が起こるか」を表す平易な行動表現で書く。絵文字と装飾記号は使わない。
"""

QUIT_LABEL = 'やめる'

# 失敗したときの選択肢。key は処理側が見分けるための名前。
FAILURE_CHOICES = (
    {'number': 1, 'key': 'retry', 'label': 'もう一度試す', 'recommended': True,
     'description': '同じ処理をもう一度行います。'},
    {'number': 2, 'key': 'help', 'label': 'やり方を見る', 'recommended': False,
     'description': 'この段で行うことと、うまくいかないときの対処を表示します。'},
    {'number': 3, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
     'description': '案内を終えます。次に起動すると、この段から続けられます。'},
)

# 段 1 で Python か Node.js が足りないときの選択肢。
PREREQUISITE_MISSING_CHOICES = (
    {'number': 1, 'key': 'open_download', 'label': '入れ方を開く', 'recommended': True,
     'description': '公式の入手ページをブラウザで開きます。入れ終わったら、この画面に戻って Enter を押すと、もう一度確かめます。'},
    {'number': 2, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
     'description': '案内を終えます。次に起動すると、この段から続けられます。'},
)

PREREQUISITE_DOWNLOAD_URLS = (
    'https://www.python.org/downloads/',
    'https://nodejs.org/',
)

STEPS = (
    {
        'id': 'prerequisites',
        'number': 1,
        'heading': '必要なものを確かめて、部品を入れる',
        'summary': 'この PC に Python と Node.js があるか調べ、足りない部品をインターネットから入れます。',
        'duration_note': '部品を入れるのに、数分かかることがあります。画面に文字が流れている間は進んでいます。',
        'help': (
            'Python 3.10 以上と Node.js 22 以上が必要です。',
            'どちらかが無いと言われたら、「入れ方を開く」を選び、公式のページから入れてください。',
            '入れ終わったら、この画面に戻って Enter を押すと、もう一度確かめます。',
            '部品を入れる段でうまくいかないときは、インターネットにつながっているか確かめてください。',
        ),
        'choices': (
            {'number': 1, 'key': 'go', 'label': '確かめて進む', 'recommended': True,
             'description': '必要なものを調べ、部品を入れます。'},
            {'number': 2, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
             'description': '案内を終えます。次に起動すると、この段から続けられます。'},
        ),
    },
    {
        'id': 'storage',
        'number': 2,
        'heading': '記録を保存する場所を決める',
        'summary': '取得した記録を、どの機械に保存するかを決めます。',
        'help': (
            'ふつうは「この PC に保存する」を選びます。',
            '「NAS など別の機械に保存する」は、すでに NAS の設定を済ませている詳しい人向けです。',
        ),
        'choices': (
            {'number': 1, 'key': 'local', 'label': 'この PC に保存する', 'recommended': True,
             'description': 'この PC の中に記録を保存します。保存先のフォルダは、次の行に表示します。'},
            {'number': 2, 'key': 'nas', 'label': 'NAS など別の機械に保存する', 'recommended': False,
             'description': '詳しい人向けです。'},
        ),
        'nas_not_ready': 'この版ではまだ案内できません。docs/operations/使い方.md を見てください。',
        'location_label': '保存先のフォルダ',
    },
    {
        'id': 'login',
        'number': 3,
        'heading': '任天堂のアカウントでログインする',
        'summary': 'ブラウザで任天堂のログイン画面を開き、ログイン後のリンクをこの画面に貼り付けます。',
        'duration_note': 'ログインの操作を含め、数分かかります。',
        'procedure': (
            'ブラウザに任天堂のログイン画面が開きます。ふだんどおりログインしてください。',
            'ログインのあと、「この人にする」と書かれたボタンが出ます。そのボタンを右クリックして、リンクをコピーします。',
            'この画面に戻り、コピーしたリンクを貼り付けて Enter を押します。貼り付けた内容は画面に表示されません。',
        ),
        'password_note': 'パスワードはこの画面に入力しません。パスワードを入れるのは、ブラウザに開く任天堂の画面だけです。',
        'help': (
            'ログイン画面が開かないときは、画面に表示されたリンクをブラウザに貼り付けて開いてください。',
            '貼り付けるのは、「この人にする」のリンクです。ログイン画面のリンクではありません。',
            'リンクや認証の情報を、チャットや GitHub に貼らないでください。',
        ),
        'choices': (
            {'number': 1, 'key': 'go', 'label': 'ブラウザでログイン画面を開く', 'recommended': True,
             'description': '任天堂のログイン画面をブラウザで開きます。'},
            {'number': 2, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
             'description': '案内を終えます。次に起動すると、この段から続けられます。'},
        ),
    },
    {
        'id': 'first_fetch',
        'number': 4,
        'heading': '最初の記録を取りに行く',
        'summary': 'いまから、これまでの記録を任天堂のサーバーから取りに行きます。',
        'duration_note': '数分から数十分かかります。画面に文字が流れている間は進んでいます。閉じずにお待ちください。',
        'help': (
            'インターネットにつながっているか確かめてください。',
            'ログインの有効期限が切れていると取得できません。そのときは、案内を起動し直して、メニューの「認証をやり直す」を選んでください。',
        ),
        'choices': (
            {'number': 1, 'key': 'go', 'label': '記録を取りに行く', 'recommended': True,
             'description': '記録の一覧を更新し、記録を取得します。'},
            {'number': 2, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
             'description': '案内を終えます。次に起動すると、この段から続けられます。'},
        ),
    },
    {
        'id': 'auto_fetch',
        'number': 5,
        'heading': '新しい記録を自動で取りに行くか決める',
        'summary': '新しい記録を、自動で取りに行くかどうかを決めます。',
        'help': (
            'macOS では、この PC にログインしている間、2分ごとに新しい記録を取りに行く設定を入れます。',
            'Windows と Linux では、記録を取りに行く画面を開いたままにしておく必要があります。その方法を次に表示します。',
        ),
        'choices': (
            {'number': 1, 'key': 'auto', 'label': 'PC が起動している間、2分ごとに新しい記録を取りに行く', 'recommended': True,
             'description': '自動で記録が増えていきます。'},
            {'number': 2, 'key': 'manual', 'label': '自分で起動したときだけ取りに行く', 'recommended': False,
             'description': '自動では取りに行きません。取りたいときに、この案内の「取得の状況を見る」などから行います。'},
        ),
        'installed_note_darwin': '2分ごとに新しい記録を取りに行く設定を入れました。',
        'manual_note': '自動では取りに行きません。',
        'watch_note_other': (
            '2分ごとに取りに行くには、次のコマンドを、この案内とは別の画面で実行し、その画面を開いたままにしてください。終了は Ctrl+C です。'
        ),
        'watch_command': 'python archive.py watch',
    },
    {
        'id': 'open_viewer',
        'number': 6,
        'heading': '記録を見る画面を開く',
        'summary': '取得した記録を見る画面を、ブラウザで開きます。',
        'phone_note': 'スマホから同じ画面を見たいときは、スマホをこの PC と同じ Wi-Fi につないでください。',
        'help': (
            '画面が開かないときは、案内を起動し直して、メニューの「記録を見る画面を開く」を選んでください。',
        ),
        'choices': (
            {'number': 1, 'key': 'open', 'label': '記録を見る画面を開く', 'recommended': True,
             'description': 'ブラウザで記録の画面を開きます。'},
            {'number': 2, 'key': 'skip', 'label': '開かずに終える', 'recommended': False,
             'description': '画面は開かず、導入を終えます。'},
        ),
    },
    {
        'id': 'drive',
        'number': 7,
        'heading': 'Google Drive にも置くか決める（任意）',
        'summary': 'AI（ChatGPT など）から記録を読めるように、記録を Google Drive にも置くかどうかを決めます。',
        'help': (
            '使う人だけが選びます。ふつうは「置かない」で問題ありません。',
        ),
        'choices': (
            {'number': 1, 'key': 'no', 'label': '置かない', 'recommended': True,
             'description': '記録はこの PC だけに保存します。'},
            {'number': 2, 'key': 'yes', 'label': '置く', 'recommended': False,
             'description': 'AI から読めるように Google Drive に置きます。'},
        ),
        'not_ready': 'この版ではまだ案内できません。docs/operations/使い方.md を見てください。',
    },
)

STEP_COUNT = len(STEPS)

# 導入が終わったあと（この PC に保存）のメニュー。key は処理側が見分ける。
RETURNING_MENU = {
    'title': 'SQuidLite3 でやりたいことを選んでください。',
    'choices': (
        {'number': 1, 'key': 'viewer', 'label': '記録を見る画面を開く', 'recommended': True,
         'description': 'ブラウザで記録の画面を開きます。'},
        {'number': 2, 'key': 'settings', 'label': '設定を変える', 'recommended': False,
         'description': '保存する場所と、自動で取りに行くかどうかを選び直します。'},
        {'number': 3, 'key': 'relogin', 'label': '認証をやり直す', 'recommended': False,
         'description': '任天堂のログイン画面をもう一度開きます。'},
        {'number': 4, 'key': 'status', 'label': '取得の状況を見る', 'recommended': False,
         'description': 'これまでに取得した記録の状況を表示します。'},
        {'number': 5, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
         'description': '案内を終えます。'},
    ),
}

# NAS に保存している環境のメニュー。導入の段は出さない。
NAS_MENU = {
    'title': 'SQuidLite3 でやりたいことを選んでください。',
    'choices': (
        {'number': 1, 'key': 'viewer', 'label': '記録を見る画面を開く', 'recommended': True,
         'description': 'ブラウザで記録の画面を開きます。'},
        {'number': 2, 'key': 'status', 'label': '取得の状況を見る', 'recommended': False,
         'description': 'NAS に取得した記録の状況を表示します。'},
        {'number': 3, 'key': 'quit', 'label': QUIT_LABEL, 'recommended': False,
         'description': '案内を終えます。'},
    ),
}

MESSAGES = {
    'step_header': '{number}/全{total}段　{heading}',
    'recommended_mark': '（おすすめ）',
    'prompt': '番号を入れて Enter を押してください（そのまま Enter でおすすめ [{default}]）: ',
    'prompt_wait_enter': '終わったら Enter を押してください: ',
    'invalid_choice': '{low} から {high} の番号を入れてください。',
    'quit_done': 'やめました。次に起動すると、続きから始まります。',
    'quit_menu_done': '終了します。',
    'resume_note': '前回の続きから始めます。済んだ段は飛ばします。',
    'all_done': '導入が終わりました。次に起動すると、メニューから選べます。',
    'step_failed': '{heading}で、うまくいきませんでした。',
    'step_failed_exit': '終了コードは {code} です。上に表示された内容を確認してください。',
    'prerequisite_python': 'Python 3.10 以上が見つかりません。',
    'prerequisite_node': 'Node.js 22 以上が見つかりません。',
    'prerequisite_ok': 'Python と Node.js を確かめました。',
    'prerequisite_opened': '入手ページを開きました。入れ終わったら、この画面に戻ってください。',
    'state_not_saved': '保存先の設定に問題があります。',
    'viewer_failed': '記録を見る画面を開けませんでした。',
    'help_title': 'やり方',
    'drive_skipped': '記録はこの PC だけに保存します。',
    'eof_quit': '入力が終わりました。案内を終えます。',
}
