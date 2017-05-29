package com.vmware.xenon.host;

import com.vmware.xenon.common.AuthorizationSetupHelper;
import com.vmware.xenon.common.ServiceHost;
import com.vmware.xenon.common.Utils;
import com.vmware.xenon.services.common.DeploymentService;
import com.vmware.xenon.services.common.DeploymentTaskService;
import com.vmware.xenon.services.common.ExampleService.ExampleServiceState;
import com.vmware.xenon.services.common.RootNamespaceService;

import java.util.logging.Level;

/**
 * Stand alone process entry point
 */
public class DemoHost extends ServiceHost {

    public static class DemoHostArguments extends Arguments {
        /**
         * The email address of a user that should be granted "admin" privileges to all services
         */
        public String adminUser;

        /**
         * The password of the adminUser
         */
        public String adminUserPassword;

        /**
         * The email address of a user that should be granted privileges just to example services
         * that they own
         */
        public String exampleUser;

        /**
         * The password of the exampleUser
         */
        public String exampleUserPassword;
    }

    private DemoHostArguments args;

    public static void main(String[] args) throws Throwable {
        DemoHost h = new DemoHost();
        h.initialize(args);
        h.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            h.log(Level.WARNING, "Host stopping ...");
            h.stop();
            h.log(Level.WARNING, "Host is stopped");
        }));
    }

    @Override
    public ServiceHost initialize(String[] args) throws Throwable {
        this.args = new DemoHostArguments();
        super.initialize(args, this.args);
        if (this.args.adminUser != null && this.args.adminUserPassword == null) {
            throw new IllegalStateException("adminUser specified, but not adminUserPassword");
        }
        if (this.args.exampleUser != null && this.args.exampleUserPassword == null) {
            throw new IllegalStateException("exampleUser specified, but not exampleUserPassword");
        }
        return this;
    }

    @Override
    public ServiceHost start() throws Throwable {
        super.start();

        startDefaultCoreServicesSynchronously();

        setAuthorizationContext(this.getSystemAuthorizationContext());

        super.startService(new RootNamespaceService());
        super.startFactory(DeploymentService.class, DeploymentService::createFactory);
        super.startFactory(DeploymentTaskService.class, DeploymentTaskService::createFactory);

        if (this.args != null) {
            if (this.args.adminUser != null) {
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail(this.args.adminUser)
                        .setUserPassword(this.args.adminUserPassword)
                        .setIsAdmin(true)
                        .start();
            }
            if (this.args.exampleUser != null) {
                AuthorizationSetupHelper.create()
                        .setHost(this)
                        .setUserEmail(this.args.exampleUser)
                        .setUserPassword(this.args.exampleUserPassword)
                        .setIsAdmin(false)
                        .setDocumentKind(Utils.buildKind(ExampleServiceState.class))
                        .start();
            }
        }

        setAuthorizationContext(null);

        return this;
    }
}