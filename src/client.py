import sys
from pymongo import errors as mongoerrors

from commands_interpreter import exec_cmd
    

def main():
    """Main function, entry point of the dfs client
    """
    while True:
        #get the next command and try to execute it
        try:
            cmd = input()
            if cmd == "quit": #if the command is "quit", exit from the main function
                sys.exit(0)
            exec_cmd(cmd)
        except KeyboardInterrupt: 
            sys.exit(0) #exit from the main function
            
if __name__ == '__main__':
    main()