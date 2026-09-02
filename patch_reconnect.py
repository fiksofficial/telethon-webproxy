import os
import re

def patch_branch():
    # Remove reconnect.py
    for f in os.listdir('.'):
        if f.endswith('_webproxy'):
            pkg = f
            break
            
    reconnect_path = os.path.join(pkg, 'reconnect.py')
    if os.path.exists(reconnect_path):
        os.remove(reconnect_path)
        print(f"Removed {reconnect_path}")

    # Patch connector_v1.py
    c1 = os.path.join(pkg, 'connector_v1.py')
    with open(c1, 'r') as f:
        content = f.read()
    content = re.sub(r'from \.reconnect import ReconnectingCarrier\n', '', content)
    content = re.sub(r'\s*use_reconnect = self\._options\.get\("reconnect", True\)\n', '\n', content)
    content = re.sub(r'\s*if use_reconnect:\n\s*self\._carrier = ReconnectingCarrier\(carrier_cls, self\._proxy_host, self\._proxy_secret\)\n\s*else:\n', '\n', content)
    # the indent is 16 spaces for self._carrier
    content = re.sub(r'\s*self\._carrier = carrier_cls\(self\._proxy_host, self\._proxy_secret\)', '\n            self._carrier = carrier_cls(self._proxy_host, self._proxy_secret)', content)
    with open(c1, 'w') as f:
        f.write(content)
        
    # Patch connector_v2.py
    c2 = os.path.join(pkg, 'connector_v2.py')
    if os.path.exists(c2):
        with open(c2, 'r') as f:
            content = f.read()
        content = re.sub(r'from \.reconnect import ReconnectingCarrier\n', '', content)
        content = re.sub(r'\s*use_reconnect = self\._options\.get\("reconnect", True\)\n', '\n', content)
        content = re.sub(r'\s*if use_reconnect:\n\s*self\._carrier = ReconnectingCarrier\(carrier_cls, self\._proxy_host, self\._proxy_secret\)\n\s*else:\n', '\n', content)
        content = re.sub(r'\s*self\._carrier = carrier_cls\(self\._proxy_host, self\._proxy_secret\)', '\n        self._carrier = carrier_cls(self._proxy_host, self._proxy_secret)', content)
        with open(c2, 'w') as f:
            f.write(content)

    # Bump version
    with open('pyproject.toml', 'r') as f:
        content = f.read()
    content = content.replace('version = "0.1.0"', 'version = "0.1.1"')
    with open('pyproject.toml', 'w') as f:
        f.write(content)

patch_branch()
print("Done patching.")
