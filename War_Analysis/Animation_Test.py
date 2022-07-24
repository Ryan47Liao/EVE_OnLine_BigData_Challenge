from matplotlib.animation import FuncAnimation
from random import randint
import matplotlib.pyplot as plt
import pandas as pd 

# function that draws each frame of the animation
def _animate(i):
    global x,y 
    pt = randint(1,9) # grab a random integer to be the next y-value in the animation
    x.append(i)
    y.append(pt)

    ax.clear()
    ax.plot(x, y)
    ax.set_xlim([0,20])
    ax.set_ylim([0,10])
    
def animate(i,window=100):
    global ax, df_war_sample,current
    col_map = {99005338:'r',1354830081:'b'}
    sample_df = df_war_sample[:current+window]
    ax.clear()
    ax.scatter(sample_df.Cord_X,sample_df.Cord_Y,
                c = sample_df.victim_alliance_id.apply(lambda x: col_map[x]))
    ax.set_xlim( df_war_sample.Cord_X.min(), df_war_sample.Cord_X.max())
    ax.set_ylim( df_war_sample.Cord_Y.min(), df_war_sample.Cord_Y.max())
    ax.set_title(f'{sample_df.tail(window).killmail_time.values[0]}->{sample_df.tail(window).killmail_time.values[-1]}')
    current+=window
    
#Pre
# create empty lists for the x and y data
# x = []
# y = []
df_war_sample = pd.read_csv("E:\Data\Eve-Online\eve_war_635778_pca.csv")
# run the animation
fig, ax = plt.subplots()

window = 500 
current = 0
ani = FuncAnimation(fig, lambda i: animate(i,window), frames=int(df_war_sample.shape[0]/window), interval=200, repeat=False)
ani.save('E:\Data\Eve-Online/war_animation.gif', writer='imagemagick', fps=60)
plt.show()
