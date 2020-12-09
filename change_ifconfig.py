import sys

def change_ifconfig(num_nodes):
    ifconfig = open("ifconfig.txt", 'r').readlines()
    j = 0
    for i in range(len(ifconfig)):
        if i == 0:
            continue
        if i == 9:
            j = 0
            continue
        if j < num_nodes:
            line = ifconfig[i]
            if line[0] == '#':
                ifconfig[i] = line[1:]
        else:
            line = ifconfig[i]
            if line[0] != '#':
                ifconfig[i] = '#' + line
        j += 1
    with open('ifconfig.txt', 'w') as file:
        file.writelines( ifconfig )

if __name__ == "__main__":
    change_ifconfig(int(sys.argv[1]))