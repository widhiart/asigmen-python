import os
import tkinter.messagebox
from tkinter import *
from tkinter import filedialog

from tkinter import ttk
from ttkthemes import themed_tk as tk

from pygame import mixer

root = Tk()
mixer.init()
root.geometry("320x320+0+0")
root.title("ArtPlay")
root.configure(bg='lightgreen')

#-------------------- function Widget ---------------------

playlist = []

def browse_file():
    global filename_path
    filename_path = filedialog.askopenfilename()
    add_to_playlist(filename_path)

def add_to_playlist(filename):
    filename = os.path.basename(filename)
    index = 0
    playlistbox.insert(index, filename)
    playlist.insert(index, filename_path)
    index += 1

def del_song():
    selected_song = playlistbox.curselection()
    selected_song = int(selected_song[0])
    playlistbox.delete(selected_song)
    playlist.pop(selected_song)


def about_us():
    tkinter.messagebox.showinfo('About ArtPlay', 'Aplikasi Musik Player dari Python Tkinter by @Widhiart')

def show_details():
    filelabel['text'] = "Playing" + ' - ' + os.path.basename(filename_path)

def play_music():
    global paused

    if paused:
        mixer.music.unpause()
        statusbar['text'] = "Playing Music" + ' - ' + os.path.basename(filename_path)
        paused = FALSE
    else:
        try:
            selected_song = playlistbox.curselection()
            selected_song = int(selected_song[0])
            play_it = playlist[selected_song]
            mixer.music.load(play_it)
            mixer.music.play()
            statusbar['text'] = "Playing Music" + ' - ' + os.path.basename(filename_path)
            show_details()
        except:
            tkinter.messagebox.showerror('File Not Found','ArtPlay could not find the file. Please check again')
            print("error")


def stop_music():
    mixer.music.stop()
    statusbar['text'] = "Music Stopped"


paused = FALSE

def pause_music():
    global paused
    paused = TRUE
    mixer.music.pause()
    statusbar['text'] = "Music Paused"

def set_vol(val):
    volume = float(val) / 100
    mixer.music.set_volume(volume)


#-------------------- MenuBar & SubMenu ---------------------

menubar = Menu(root)
root.config(menu=menubar)
subMenu = Menu(menubar, tearoff=0)

menubar.add_cascade(label="File", menu=subMenu)
subMenu.add_command(label="Open", command=browse_file)
subMenu.add_command(label="Close", command=root.destroy)

subMenu = Menu(menubar)
menubar.add_cascade(label="Help", menu=subMenu)
subMenu.add_command(label="About Us", command=about_us)


filelabel = Label(root, text='Music Player BY @ArtPlay', bg='lightgreen', font='Arial 12 bold', fg='blue')
filelabel.pack(pady=5)

#-------------------- Button & ListBox ---------------------

bottomframe = Frame(root)
bottomframe.pack(side=BOTTOM, fill=X, padx=5)

middleframe = Frame(root, bg='lightgreen')
middleframe.pack(side=TOP,fill=X, padx=3, pady=10)

botmidframe = Frame(root, bg='lightgreen')
botmidframe.pack(side=RIGHT)

leftframe = Frame(root, bg='lightgreen')
leftframe.pack(side=LEFT, fil=X, padx=10)

#-------------------- Button & ListBox ---------------------
playphoto = PhotoImage(file='asset/play.png')
btnplay = ttk.Button(middleframe, image=playphoto, command=play_music)
btnplay.pack(side=LEFT, fill=X, padx=23)

pausephoto = PhotoImage(file='asset/pause.png')
btnpause = ttk.Button(middleframe, image=pausephoto, command=pause_music)
btnpause.pack(side=LEFT, fill=X, padx=23)

stopphoto = PhotoImage(file='asset/stop-button.png')
btnstop = ttk.Button(middleframe, image=stopphoto, command=stop_music)
btnstop.pack(side=LEFT, fill=X,padx=23)

scale = ttk.Scale(botmidframe, from_=0, to=100, orient=VERTICAL, command=set_vol)
scale.set(70)
mixer.music.set_volume(0.7)
scale.pack()

playlistbox = Listbox(leftframe)
playlistbox.pack()

addBtn = ttk.Button(leftframe, text="Add Song", command=browse_file)
addBtn.pack(side=LEFT, padx=18, pady=10)
delBtn = ttk.Button(leftframe, text="Remove", command=del_song)
delBtn.pack(side=RIGHT, padx=18, pady=10)

statusbar = Label(bottomframe, text="SELAMAT DATANG DI ArtPlay", relief=SUNKEN, anchor=W, font='Times 12 bold')
statusbar.pack(side=BOTTOM, fill=X)

root.mainloop()