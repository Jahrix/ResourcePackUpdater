package cn.zbx1425.resourcepackupdater.mixin;

import cn.zbx1425.resourcepackupdater.ResourcePackUpdater;
import cn.zbx1425.resourcepackupdater.drm.AssetEncryption;
import cn.zbx1425.resourcepackupdater.drm.ServerLockRegistry;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.packs.PackResources;
import net.minecraft.server.packs.PackType;
import net.minecraft.server.packs.PathPackResources;
import net.minecraft.server.packs.resources.IoSupplier;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Final;
import org.spongepowered.asm.mixin.Shadow;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;
import java.nio.file.Files;
import java.nio.file.Path;

@Mixin(PathPackResources.class)
public abstract class FolderPackResourcesMixin {

    @Unique
    private Path canonicalRoot;

    @Unique
    private Path getCanonicalRoot() {
        if (canonicalRoot == null) {
            try {
                canonicalRoot = root.toRealPath();
            } catch (IOException e) {
                canonicalRoot = root.toAbsolutePath().normalize();
            }
        }
        return canonicalRoot;
    }

    @Shadow @Final
    private Path root;

    @Unique
    private boolean isSelectedPackRoot() {
        File selectedPackRoot = ResourcePackUpdater.CONFIG.packBaseDirFile.value;
        if (selectedPackRoot == null) {
            return false;
        }
        try {
            return getCanonicalRoot().equals(selectedPackRoot.getCanonicalFile().toPath());
        } catch (IOException e) {
            return getCanonicalRoot().equals(selectedPackRoot.toPath().toAbsolutePath().normalize());
        }
    }

    @Unique
    private static Path resolveInside(Path base, String relative) {
        Path resolved = base.resolve(relative).normalize();
        return resolved.startsWith(base) ? resolved : null;
    }

    @Unique
    private static IoSupplier<InputStream> encryptedInputSupplier(Path filePath) {
        return () -> AssetEncryption.wrapInputStream(new FileInputStream(filePath.toFile()));
    }

    @Inject(method = "getRootResource", at = @At("HEAD"), cancellable = true)
    private void getRootResource(String[] pathSegments, CallbackInfoReturnable<IoSupplier<InputStream>> cir) {
        if (isSelectedPackRoot()) {
            String resourcePath = String.join("/", pathSegments);
            if (ServerLockRegistry.shouldRefuseProvidingFile(resourcePath)) {
                cir.setReturnValue(null);
                cir.cancel();
                return;
            }
            Path resolved = resolveInside(root, resourcePath);
            if (resolved != null && Files.isRegularFile(resolved)) {
                cir.setReturnValue(encryptedInputSupplier(resolved));
                cir.cancel();
            }
        }
    }

    @Inject(method = "getResource", at = @At("HEAD"), cancellable = true)
    private void getResource(PackType type, ResourceLocation resourceLocation, CallbackInfoReturnable<IoSupplier<InputStream>> cir) {
        if (isSelectedPackRoot()) {
            if (ServerLockRegistry.shouldRefuseProvidingFile(resourceLocation.getPath())) {
                cir.setReturnValue(null);
                cir.cancel();
                return;
            }
            Path namespaceRoot = root.resolve(type.getDirectory())
                    .resolve(resourceLocation.getNamespace()).normalize();
            if (!namespaceRoot.startsWith(root)) {
                cir.setReturnValue(null);
                cir.cancel();
                return;
            }
            Path resolved = resolveInside(namespaceRoot, resourceLocation.getPath());
            if (resolved != null && Files.isRegularFile(resolved)) {
                cir.setReturnValue(encryptedInputSupplier(resolved));
                cir.cancel();
            }
        }
    }

    @Inject(method = "listResources", at = @At("HEAD"), cancellable = true)
    private void listResources(PackType type, String namespace, String path,
                               PackResources.ResourceOutput resourceOutput, CallbackInfo ci) {
        if (isSelectedPackRoot()) {
            if (ServerLockRegistry.shouldRefuseProvidingFile(null)) {
                ci.cancel();
            }
        }
    }

    @Inject(method = "getNamespaces", at = @At("HEAD"), cancellable = true)
    private void getNamespaces(PackType type, CallbackInfoReturnable<Set<String>> cir) {
        if (isSelectedPackRoot()) {
            if (ServerLockRegistry.shouldRefuseProvidingFile(null)) {
                cir.setReturnValue(Collections.emptySet());
                cir.cancel();
            }
        }
    }
}
