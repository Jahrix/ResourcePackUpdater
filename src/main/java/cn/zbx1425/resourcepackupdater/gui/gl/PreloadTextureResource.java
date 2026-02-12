package cn.zbx1425.resourcepackupdater.gui.gl;

import cn.zbx1425.resourcepackupdater.ResourcePackUpdater;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.packs.PackResources;
import net.minecraft.server.packs.PackType;
import net.minecraft.server.packs.resources.Resource;
import net.minecraft.server.packs.resources.IoSupplier;
import net.minecraft.server.packs.metadata.MetadataSectionSerializer;

import java.io.IOException;
import java.io.InputStream;

import java.util.Collections;
import java.util.Set;

public class PreloadTextureResource extends Resource {

    private static final PackResources DUMMY_PACK = new PackResources() {
        @Override
        public IoSupplier<InputStream> getRootResource(String... pathSegments) {
            return null;
        }

        @Override
        public IoSupplier<InputStream> getResource(PackType type, ResourceLocation resourceLocation) {
            return null;
        }

        @Override
        public void listResources(PackType type, String namespace, String path, ResourceOutput resourceOutput) {
            // Unused
        }

        @Override
        public Set<String> getNamespaces(PackType type) {
            return Collections.emptySet();
        }

        @Override
        public <T> T getMetadataSection(MetadataSectionSerializer<T> metadataSectionSerializer) {
            return null;
        }

        @Override
        public String packId() {
            return ResourcePackUpdater.MOD_ID + "_preload";
        }

        @Override
        public void close() {
            // No resources to release
        }
    };

    public PreloadTextureResource(ResourceLocation resourceLocation) {
        super(DUMMY_PACK, () -> openResource(resourceLocation));
    }

    private static InputStream openResource(ResourceLocation resourceLocation) throws IOException {
        InputStream stream = PreloadTextureResource.class.getResourceAsStream(
                "/assets/" + resourceLocation.getNamespace() + "/" + resourceLocation.getPath()
        );
        if (stream == null) {
            throw new IOException("Missing preload resource: " + resourceLocation);
        }
        return stream;
    }
}
