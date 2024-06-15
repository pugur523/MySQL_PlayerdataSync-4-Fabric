package com.pugur.playerdata.mixins;

import com.pugur.playerdata.MySQLPlayerdataSync;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.network.ServerPlayerEntity;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

import java.util.Collection;

@Mixin(MinecraftServer.class)
public class MinecraftServerMixin {

    @Unique
    private boolean flag = true;

    @Inject(method = "stop", at = @At("HEAD"))
    private void onServerStop(CallbackInfo info) {
        if (flag) {
            MinecraftServer server = (MinecraftServer) (Object) this;
            Collection<ServerPlayerEntity> collection = server.getPlayerManager().getPlayerList();
            for (ServerPlayerEntity player : collection) {
                MySQLPlayerdataSync.savePlayerDataToDatabase(player);
            }
        }
        flag = false;
    }
}


