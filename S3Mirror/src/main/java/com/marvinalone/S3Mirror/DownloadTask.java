package com.marvinalone.S3Mirror;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class DownloadTask implements Runnable {
    private final static Log s_log = LogFactory.getLog(DownloadTask.class);
    private final static long PROGRESS_MESSAGE_INTERVAL = 10 * 1000;
    
    private final AmazonS3 m_s3;
    private final S3ObjectSummary m_s3objectSummary;
    private final File m_destination;
    private final boolean m_remove;
    
    public DownloadTask(
        final AmazonS3 s3,
        final S3ObjectSummary s3objectSummary,
        final String prefix,
        final String localDestination,
        final boolean remove
    ) {
        m_s3 = s3;
        m_s3objectSummary = s3objectSummary;
        m_remove = remove;
        
        // create destination
        if(!s3objectSummary.getKey().startsWith(prefix))
            throw new IllegalArgumentException("S3 object's key doesn't start with the specified prefix.");
        String destinationPath =
            localDestination +
            "/" +
            s3objectSummary.getKey().substring(prefix.length());
        while(destinationPath.contains("//"))
            destinationPath = destinationPath.replace("//", "/");
        destinationPath = destinationPath.replace('/', File.separatorChar);
        m_destination = new File(destinationPath);
        m_destination.getParentFile().mkdirs();
    }

    public void run() {
        s_log.info(
            String.format(
                "Starting download of s3://%s/%s to %s",
                m_s3objectSummary.getBucketName(),
                m_s3objectSummary.getKey(),
                m_destination.getAbsolutePath()));
        
        try {
            runReally();
        } catch(final Exception e) {    // gotta catch 'em all! 
            s_log.error("Unhandled error while downloading", e);
        }
    }
    
    private void runReally() throws IOException {
        final S3Object object =
            m_s3.getObject(
                m_s3objectSummary.getBucketName(),
                m_s3objectSummary.getKey());

        final long totalSize =
            object.getObjectMetadata().getContentLength();
        if(m_destination.length() == totalSize) {
            s_log.info(
                String.format(
                    "Skipping the download of s3://%s/%s, since the destination already exists and has the correct size.",
                    m_s3objectSummary.getBucketName(),
                    m_s3objectSummary.getKey()));
        } else if(m_destination.length() > totalSize) {
            s_log.warn(
                String.format(
                    "File %s already exists, and is larger than it will be after the download. Redownloading file.",
                    m_destination.getAbsolutePath()));
            m_destination.delete();
        } else {
            // download the file
            try (
                final OutputStream out = new FileOutputStream(m_destination, true);
                final InputStream in = object.getObjectContent()
            ) {
                long downloadedSize = 0;
                long lastProgressMessage = System.currentTimeMillis();
                long lastDownloadedSize = 0;
    
                // skip over the stuff that's already in the file
                long toSkip = m_destination.length();
                while(toSkip > 0) {
                    final long skipped = in.skip(Math.min(toSkip, 1024 * 1024));
                    toSkip -= skipped;
                    
                    downloadedSize += skipped;
                    final long currentTime = System.currentTimeMillis();
                    if(currentTime - lastProgressMessage >= PROGRESS_MESSAGE_INTERVAL) {
                        final double fractionDownloaded =
                            (double)downloadedSize / (double)totalSize;
                        final double bytesSinceLast =
                            downloadedSize - lastDownloadedSize;
                        final double secondsSinceLast =
                            (currentTime - lastProgressMessage) / 1000.0;
                        s_log.info(
                            String.format(
                                "%s downloaded %.0f%% (%.1f kb/s) (skipping already downloaded part)",
                                m_destination.getAbsolutePath(),
                                100.0 * fractionDownloaded,
                                (bytesSinceLast / 1024.0) / secondsSinceLast));
                        lastDownloadedSize = downloadedSize;
                        lastProgressMessage = currentTime;
                    }
                }
                
                final byte[] buffer = new byte[1024 * 1024];
                while(true) {
                    final int read = in.read(buffer);
                    if(read == -1)
                        break;
                    out.write(buffer, 0, read);
                    
                    downloadedSize += read;
                    final long currentTime = System.currentTimeMillis();
                    if(currentTime - lastProgressMessage >= PROGRESS_MESSAGE_INTERVAL) {
                        final double fractionDownloaded =
                            (double)downloadedSize / (double)totalSize;
                        final double bytesSinceLast =
                            downloadedSize - lastDownloadedSize;
                        final double secondsSinceLast =
                            (currentTime - lastProgressMessage) / 1000.0;
                        s_log.info(
                            String.format(
                                "%s downloaded %.0f%% (%.1f kb/s)",
                                m_destination.getAbsolutePath(),
                                100.0 * fractionDownloaded,
                                (bytesSinceLast / 1024.0) / secondsSinceLast));
                        lastDownloadedSize = downloadedSize;
                        lastProgressMessage = currentTime;
                    }
                }
            }
            
            s_log.info(
                String.format(
                    "%s downloaded 100%%", m_destination.getAbsolutePath()));
        }
        
        if(m_remove) {
            m_s3.deleteObject(
                m_s3objectSummary.getBucketName(), 
                m_s3objectSummary.getKey());
            s_log.info(
                String.format(
                    "Deleted s3://%s/%s after a complete download",
                    m_s3objectSummary.getBucketName(),
                    m_s3objectSummary.getKey()));
        }
    }
}
