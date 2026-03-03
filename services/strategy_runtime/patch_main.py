with open('main.py', 'r') as f:
    text = f.read()

patch = r'''
        if not rows:
            logger.warning(f⚠️
