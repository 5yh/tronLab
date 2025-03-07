import base58

def convertTronAddresstoEvmAddress(tronAddress=''):
    prefix='0x'
    return prefix+base58.b58decode(tronAddress).hex()[2:42].lower()
if __name__ =="__main__":
    print(convertTronAddresstoEvmAddress('TUbrNqiVa7PiGLtAfddxKdxKMjjfowFCic'))