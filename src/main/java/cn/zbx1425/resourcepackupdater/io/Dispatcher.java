package cn.zbx1425.resourcepackupdater.io;

import cn.zbx1425.resourcepackupdater.Config;
import cn.zbx1425.resourcepackupdater.ResourcePackUpdater;
import cn.zbx1425.resourcepackupdater.gui.gl.GlHelper;
import cn.zbx1425.resourcepackupdater.gui.GlProgressScreen;
import cn.zbx1425.resourcepackupdater.io.network.DownloadDispatcher;
import cn.zbx1425.resourcepackupdater.io.network.DownloadTask;
import cn.zbx1425.resourcepackupdater.io.network.PackOutputStream;
import cn.zbx1425.resourcepackupdater.io.network.RemoteMetadata;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Dispatcher {

    private RemoteMetadata remoteMetadata;
    private LocalMetadata localMetadata;

    public boolean runSync(String baseDir, Config.SourceProperty source, ProgressReceiver cb) throws Exception {
        try {
            if (source.baseUrl.isEmpty()) {
                throw new IOException("There is no source configured. Install the config file to your config folder!");
            }

            cb.printLog(ResourcePackUpdater.MOD_NAME + " v" + ResourcePackUpdater.MOD_VERSION);
            cb.printLog(ResourcePackUpdater.ORIGINAL_CREDIT);
            cb.printLog("Server: " + source.baseUrl);
            cb.printLog("Target: " + baseDir);
            cb.printLog("");

            if (source.hasArchive) {
                return runArchiveSync(baseDir, source, cb);
            }
            return runMetadataSync(baseDir, source, cb);
        } catch (GlHelper.MinecraftStoppingException ex) {
            throw ex;
        } catch (Exception ex) {
            cb.setException(ex);
            return false;
        }
    }

    private boolean runMetadataSync(String baseDir, Config.SourceProperty source, ProgressReceiver cb) throws Exception {
        localMetadata = new LocalMetadata(baseDir);
        remoteMetadata = new RemoteMetadata(source.baseUrl);

        byte[] remoteChecksum = null;

        if (source.hasDirHash) {
            cb.printLog("Downloading remote directory checksum ...");
            remoteChecksum = remoteMetadata.fetchDirChecksum(cb);
            cb.amendLastLog("Done");
            cb.printLog("Remote directory checksum is " + Hex.encodeHexString(remoteChecksum));
        } else {
            cb.printLog("This server does not have a directory checksum.");
            cb.printLog("Downloading remote metadata ...");
            remoteMetadata.fetch(cb);
            cb.amendLastLog("Done");
            cb.setProgress(0, 0);
        }
        // Now, either checksum or full metadata is fetched, with the encryption switch.

        cb.printLog("Scanning local files ...");
        localMetadata.scanDir(remoteMetadata.encrypt, cb);
        cb.amendLastLog("Done");
        byte[] localChecksum = localMetadata.getDirChecksum();
        cb.printLog("Local directory checksum is " + Hex.encodeHexString(localChecksum));

        if (localMetadata.files.size() < 1) {
            cb.printLog("The resource pack for the server is being downloaded.");
            cb.printLog("This is going to take a while. Sit back and relax!");
        }
        if (remoteChecksum != null) {
            if (Arrays.equals(localChecksum, remoteChecksum)) {
                cb.printLog("All files are up to date.");
                cb.setProgress(1, 1);
                cb.printLog("");
                cb.printLog("Done! Thank you.");
                return true;
            } else {
                // We haven't fetched the full metadata yet, do it now.
                cb.printLog("Downloading remote metadata ...");
                remoteMetadata.fetch(cb);
                cb.amendLastLog("Done");
                cb.setProgress(0, 0);
            }
        }

        List<String> dirsToCreate = localMetadata.getDirsToCreate(remoteMetadata);
        List<String> dirsToDelete = localMetadata.getDirsToDelete(remoteMetadata);
        List<String> filesToCreate = localMetadata.getFilesToCreate(remoteMetadata);
        List<String> filesToUpdate = localMetadata.getFilesToUpdate(remoteMetadata);
        List<String> filesToDelete = localMetadata.getFilesToDelete(remoteMetadata);
        cb.printLog(String.format("Found %-3d new directories, %-3d to delete.",
                dirsToCreate.size(), dirsToDelete.size()));
        cb.printLog(String.format("Found %-3d new files, %-3d to update, %-3d to delete.",
                filesToCreate.size(), filesToUpdate.size(), filesToDelete.size()));

        cb.printLog("Creating & deleting directories and files ...");
        for (String dir : dirsToCreate) {
            Files.createDirectories(Paths.get(baseDir, dir));
        }
        for (String file : filesToDelete) {
            Files.deleteIfExists(Paths.get(baseDir, file));
        }
        for (String dir : dirsToDelete) {
            Path dirPath = Paths.get(baseDir, dir);
            if (Files.isDirectory(dirPath)) FileUtils.deleteDirectory(dirPath.toFile());
        }
        cb.amendLastLog("Done");

        remoteMetadata.beginDownloads(cb);
        cb.printLog("Downloading files ...");
        DownloadDispatcher downloadDispatcher = new DownloadDispatcher(cb);
        for (String file : Stream.concat(filesToCreate.stream(), filesToUpdate.stream()).toList()) {
            DownloadTask task = new DownloadTask(downloadDispatcher,
                    remoteMetadata.baseUrl + "/dist/" + file, file, remoteMetadata.files.get(file).size);
            downloadDispatcher.dispatch(task, () -> new PackOutputStream(Paths.get(baseDir, file),
                    remoteMetadata.encrypt, localMetadata.hashCache, remoteMetadata.files.get(file).hash));
        }
        while (!downloadDispatcher.tasksFinished()) {
            downloadDispatcher.updateSummary();
            ((GlProgressScreen)cb).redrawScreen(true);
            Thread.sleep(1000 / 30);
        }
        remoteMetadata.downloadedBytes += downloadDispatcher.downloadedBytes;
        downloadDispatcher.close();
        localMetadata.saveHashCache();

        cb.setInfo("", "");
        cb.setProgress(1, 1);
        cb.printLog("");
        remoteMetadata.endDownloads(cb);
        cb.printLog("Done! Thank you.");
        return true;
    }

    private boolean runArchiveSync(String baseDir, Config.SourceProperty source, ProgressReceiver cb) throws Exception {
        cb.printLog("Using archive-manifest source mode.");
        cb.printLog("Loading latest pack manifest ...");
        ArchiveManifest manifest = fetchArchiveManifest(source.baseUrl);
        cb.amendLastLog("Done");
        cb.printLog("Latest version: " + manifest.version + " (" + manifest.updatedAt + ")");

        Path basePath = Paths.get(baseDir);
        Path statePath = basePath.resolve("updater_manifest_state.json");
        if (isArchiveUpToDate(basePath, statePath, manifest)) {
            cb.printLog("All files are up to date.");
            cb.setProgress(1, 1);
            cb.printLog("");
            cb.printLog("Done! Thank you.");
            return true;
        }

        cb.printLog("Downloading pack archive ...");
        Path tempZip = Files.createTempFile("rpu-pack-", ".zip");
        Path tempExtract = null;
        try {
            downloadArchiveToFile(manifest, tempZip, cb);
            cb.amendLastLog("Done");
            cb.printLog("Verifying archive checksum ...");
            verifyArchiveChecksum(tempZip, manifest.sha1);
            cb.amendLastLog("Done");

            cb.printLog("Extracting archive ...");
            tempExtract = extractZipToTempDir(tempZip);
            Path packRoot = findPackRoot(tempExtract);
            cb.amendLastLog("Done");

            cb.printLog("Applying resource pack files ...");
            replaceDirectory(packRoot, basePath);
            writeArchiveState(statePath, manifest);
            cb.amendLastLog("Done");
        } finally {
            Files.deleteIfExists(tempZip);
            if (tempExtract != null && Files.isDirectory(tempExtract)) {
                FileUtils.deleteDirectory(tempExtract.toFile());
            }
        }

        cb.setInfo("", "");
        cb.setProgress(1, 1);
        cb.printLog("");
        cb.printLog("Done! Thank you.");
        return true;
    }

    private ArchiveManifest fetchArchiveManifest(String manifestUrl) throws Exception {
        HttpResponse<InputStream> response = DownloadTask.sendHttpRequest(URI.create(manifestUrl));
        if (response.statusCode() >= 400) {
            throw new IOException("Server returned HTTP " + response.statusCode() + " for manifest URL: " + manifestUrl);
        }
        String body;
        try (InputStream inputStream = DownloadTask.unwrapHttpResponse(response)) {
            body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        JsonObject manifestObj = ResourcePackUpdater.JSON_PARSER.parse(body).getAsJsonObject();
        if (!manifestObj.has("url")) {
            throw new IOException("Manifest is missing 'url': " + body);
        }
        return new ArchiveManifest(
                manifestObj.has("name") ? manifestObj.get("name").getAsString() : "Unknown Pack",
                manifestObj.has("version") ? manifestObj.get("version").getAsString() : "unknown",
                manifestObj.get("url").getAsString(),
                manifestObj.has("sha1") ? manifestObj.get("sha1").getAsString() : "",
                manifestObj.has("sizeBytes") ? manifestObj.get("sizeBytes").getAsLong() : 0L,
                manifestObj.has("updatedAt") ? manifestObj.get("updatedAt").getAsString() : "unknown time"
        );
    }

    private void downloadArchiveToFile(ArchiveManifest manifest, Path targetZip, ProgressReceiver cb) throws Exception {
        HttpResponse<InputStream> response = DownloadTask.sendHttpRequest(URI.create(manifest.url));
        if (response.statusCode() >= 400) {
            throw new IOException("Server returned HTTP " + response.statusCode() + " while downloading archive: " + manifest.url);
        }
        long totalBytes = Long.parseLong(response.headers().firstValue("Content-Length").orElse(Long.toString(manifest.sizeBytes)));
        long downloadedBytes = 0;

        try (InputStream inputStream = new BufferedInputStream(DownloadTask.unwrapHttpResponse(response));
             OutputStream outputStream = Files.newOutputStream(targetZip)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, read);
                downloadedBytes += read;
                if (totalBytes > 0) {
                    cb.setProgress(downloadedBytes * 1f / totalBytes, 0);
                    cb.setInfo(String.format("%.2f%%", downloadedBytes * 100f / totalBytes),
                            String.format(": %5d KiB / %5d KiB", downloadedBytes / 1024, totalBytes / 1024));
                } else {
                    cb.setInfo("", String.format(": %5d KiB downloaded", downloadedBytes / 1024));
                }
            }
        }
    }

    private void verifyArchiveChecksum(Path archivePath, String expectedSha1) throws Exception {
        if (expectedSha1 == null || expectedSha1.isBlank()) {
            return;
        }
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        try (InputStream inputStream = Files.newInputStream(archivePath)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        String actualSha1 = Hex.encodeHexString(digest.digest());
        if (!actualSha1.equalsIgnoreCase(expectedSha1)) {
            throw new IOException("Archive checksum mismatch. Expected " + expectedSha1 + " but got " + actualSha1);
        }
    }

    private Path extractZipToTempDir(Path zipPath) throws IOException {
        Path extractDir = Files.createTempDirectory("rpu-unzip-");
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipPath))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                Path output = extractDir.resolve(entry.getName()).normalize();
                if (!output.startsWith(extractDir)) {
                    throw new IOException("Invalid ZIP entry path: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(output);
                } else {
                    Path parent = output.getParent();
                    if (parent != null) Files.createDirectories(parent);
                    Files.copy(zis, output, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
        return extractDir;
    }

    private Path findPackRoot(Path extractDir) throws IOException {
        if (Files.isRegularFile(extractDir.resolve("pack.mcmeta"))) {
            return extractDir;
        }
        List<Path> topLevelEntries;
        try (Stream<Path> list = Files.list(extractDir)) {
            topLevelEntries = list.toList();
        }
        if (topLevelEntries.size() == 1 && Files.isDirectory(topLevelEntries.get(0))) {
            Path nestedRoot = topLevelEntries.get(0);
            if (Files.isRegularFile(nestedRoot.resolve("pack.mcmeta"))) {
                return nestedRoot;
            }
        }
        return extractDir;
    }

    private void replaceDirectory(Path sourceRoot, Path targetRoot) throws IOException {
        if (Files.exists(targetRoot)) {
            FileUtils.deleteDirectory(targetRoot.toFile());
        }
        Files.createDirectories(targetRoot);
        try (Stream<Path> walk = Files.walk(sourceRoot)) {
            for (Path sourcePath : walk.toList()) {
                Path relativePath = sourceRoot.relativize(sourcePath);
                if (relativePath.toString().isEmpty()) continue;
                Path targetPath = targetRoot.resolve(relativePath.toString());
                if (Files.isDirectory(sourcePath)) {
                    Files.createDirectories(targetPath);
                } else {
                    Path parent = targetPath.getParent();
                    if (parent != null) Files.createDirectories(parent);
                    Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private boolean isArchiveUpToDate(Path basePath, Path statePath, ArchiveManifest manifest) {
        if (!Files.isRegularFile(basePath.resolve("pack.mcmeta")) || !Files.isRegularFile(statePath)) {
            return false;
        }
        try {
            JsonObject state = ResourcePackUpdater.JSON_PARSER.parse(Files.readString(statePath)).getAsJsonObject();
            if (!manifest.sha1.isBlank() && state.has("sha1")) {
                return manifest.sha1.equalsIgnoreCase(state.get("sha1").getAsString());
            }
            return state.has("version") && manifest.version.equals(state.get("version").getAsString());
        } catch (Exception ignored) {
            return false;
        }
    }

    private void writeArchiveState(Path statePath, ArchiveManifest manifest) throws IOException {
        JsonObject state = new JsonObject();
        state.addProperty("name", manifest.name);
        state.addProperty("version", manifest.version);
        state.addProperty("url", manifest.url);
        state.addProperty("sha1", manifest.sha1);
        state.addProperty("sizeBytes", manifest.sizeBytes);
        state.addProperty("updatedAt", manifest.updatedAt);
        Path parent = statePath.getParent();
        if (parent != null) Files.createDirectories(parent);
        Files.writeString(statePath, new GsonBuilder().setPrettyPrinting().create().toJson(state), StandardCharsets.UTF_8);
    }

    private static final class ArchiveManifest {
        private final String name;
        private final String version;
        private final String url;
        private final String sha1;
        private final long sizeBytes;
        private final String updatedAt;

        private ArchiveManifest(String name, String version, String url, String sha1, long sizeBytes, String updatedAt) {
            this.name = name;
            this.version = version;
            this.url = url;
            this.sha1 = sha1;
            this.sizeBytes = sizeBytes;
            this.updatedAt = updatedAt;
        }
    }
}
