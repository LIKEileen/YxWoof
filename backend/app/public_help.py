"""Public, synthetic contact. No auth, database, model or private summaries."""
def public_help():
    return {
        "availability": "demo_public",
        "entry": {"contact": "support@yxwoof.example", "label": "YxWoof 客服邮箱", "virtual": True},
        "summary": "",
        "pause_confirmed": False,
    }
