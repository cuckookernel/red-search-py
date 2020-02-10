"""Monitor redis instance"""

import redis
import datetime as dt


def main():
    """Bring up monitor,  print every command"""
    red = redis.Redis(host='localhost', port=6379, db=0)

    with red.monitor() as m:
        for cmd in m.listen():
            cmd_tm = dt.datetime.fromtimestamp( cmd['time'] )
            print( f"{cmd_tm.strftime('%H:%M:%S.%f')}\t{cmd['command']}")


if __name__ == "__main__":
    main()
