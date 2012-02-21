#!/usr/bin/python

import transmissionrpc
import os
import time

FREE_SPACE_LIMIT = 100 * 1024 * 1024;

def main():
    tc = transmissionrpc.Client('localhost', port=9091, user="transmission", password="/racing/done")

    # remove all done torrents where the seed ratio has reached 1.1
    for torrent_id, torrent in tc.list().iteritems():
        if torrent.ratio >= 1.1 and torrent.fields["leftUntilDone"] == 0:
            tc.remove(torrent_id)
            time.sleep(2)

    # find how much space we have
    s = os.statvfs("/var/lib/transmission-daemon")
    free_space = s.f_bsize * s.f_bavail

    # if we're below 100MB free space, kill a torrent that's done downloading
    # specifically, kill the torrent that has the highest ratio
    if free_space < FREE_SPACE_LIMIT:
        print "Space below 100 MB, trying to kill a seeding torrent"
        doomed_id = None
        doomed_ratio = -1.0
        for torrent_id, torrent in tc.list().iteritems():
            if torrent.fields["leftUntilDone"] == 0 and torrent.ratio > doomed_ratio:
                doomed_id = torrent_id
                doomed_ratio = torrent.ratio
        if doomed_id:
            print "Found torrent %s to kill with ratio %f" % (doomed_id, doomed_ratio)
            tc.remove(doomed_id)
        else:
            # we're out of space, but there is nothing to remove
            # better pause all the things
            downloading_torrent_ids = [torrent_id for (torrent_id, torrent) in tc.list().iteritems() if torrent.status == "downloading"]
            if len(downloading_torrent_ids) > 0:
                print "Found no torrent to kill. Stopping all the things."
                tc.stop(downloading_torrent_ids)
        return

    # make some decisions about which torrents to run and which ones to pause
    # calculate space available, which is free space plus the space of all finished torrents
    torrents = tc.list()
    space_available = free_space + sum([t.fields["sizeWhenDone"] for t in torrents.values() if t.fields["leftUntilDone"] == 0]) - FREE_SPACE_LIMIT
    
    # now schedule torrents to use up that space
    # giving first preference to torrents that are already running
    # giving second preference to torrents that are close to being done
    def torrent_cmp(left, right):
        left_id, left_t = left
        right_id, right_t = right
        
        if left_t.status == "downloading" and right_t.status != "downloading":
            return -1
        if left_t.status != "downloading" and right_t.status == "downloading":
            return 1
        
        left_left = left_t.fields["leftUntilDone"]
        right_left = right_t.fields["leftUntilDone"]
        if left_left < right_left:
            return -1
        if left_left > right_left:
            return 1

        return 0

    incomplete_torrents = [(tid, t) for (tid, t) in torrents.iteritems() if t.status == "downloading" or t.fields["leftUntilDone"] > 0]
    incomplete_torrents.sort(cmp = torrent_cmp)

    scheduled_torrent_ids = set()
    for tid, t in incomplete_torrents:
        left = t.fields["leftUntilDone"]
        if left < space_available:
            # print "Space available: %i scheduling torrent %s with %i left" % (space_available, tid, left)
            scheduled_torrent_ids.add(tid)
            space_available -= left

    # now we know which torrents we want to have running, and which ones are supposed to be stopped
    # make it so!
    to_be_stopped = set()
    to_be_started = set()
    for tid, t in torrents.iteritems():
        if t.status == "downloading" or t.fields["leftUntilDone"] > 0:
            if t.status == "downloading" and not tid in scheduled_torrent_ids:
                to_be_stopped.add(tid)
            if t.status != "downloading" and tid in scheduled_torrent_ids:
                to_be_started.add(tid)
    if len(to_be_stopped) > 0:
        print "Stopping %i torrents" % len(to_be_stopped)
        tc.stop(list(to_be_stopped))
    if len(to_be_started) > 0:
        print "Starting %i torrents" % len(to_be_started)
        tc.start(list(to_be_started))

if __name__=="__main__":
    main()
