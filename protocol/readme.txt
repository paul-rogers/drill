This project contains the protobuf definition files used by Drill.

The java sources are generated into src/main/java and checked in.

To regenerate the sources after making changes to .proto files
---------------------------------------------------------------
1. Ensure that the protobuf 'protoc' tool (version 3.6.1) is
   in your PATH (you may need to download and build it first). You can
   download it from https://github.com/protocolbuffers/protobuf/releases.

   You must use version the protoc version that matches the runtime libraries
   which Drill depends upon. Otherwise you will get the following error:

   [INFO] Protobuf dependency version 3.6.1
   [INFO] 'protoc' executable version 3.10.1
   [ERROR] Failed to execute goal com.github.igor-petruk.protobuf:protobuf-maven-plugin:0.6.5:run (default)
           on project drill-protocol: Protobuf installation version does not match Protobuf library version

   Note: If generating sources on MAC follow below instructions (note these
   no longer work as of 11, 2019; Brew changed its version system and protoc
   is no longer available.)

    a) Download and install "brew"
       Command: /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"

    b) Download and install "protoc"
       Command: brew install protobuf361  --- installs protobuf for version 3.6.1
                          brew install protobuf     --- installs latest protobuf version

    c) Check the version of "protoc"
       Command: protoc --version

    d) Follow steps 2 and 3 below

   Steps that do work are:

    a) Download protoc-3.6.1-osx-x86_64.zip from
       https://github.com/protocolbuffers/protobuf/releases

    b) Expand the directory and move it to a convenient location
       such as ~/bin.

    c) Add ~/bin/protoc-3.6.1-osx-x86_64/bin to your PATH:
       export PATH=$PATH:~/bin/protoc-3.6.1-osx-x86_64/bin

    d) Check the version of "protoc"
       Command: protoc --version

    e) Follow steps 2 and 3 below

2. In protocol dir, run "mvn process-sources -P proto-compile" or "mvn clean install -P proto-compile".

3. Check in the new/updated files.

---------------------------------------------------------------
If changes are made to the DrillClient's protobuf, you would need to regenerate the sources for the C++ client as well.
Steps for regenerating the sources are available https://github.com/apache/drill/blob/master/contrib/native/client/

You can use any of the following platforms specified in the above location to regenerate the protobuf sources:
readme.linux	: Regenerating on Linux
readme.macos	: Regenerating on MacOS
readme.win.txt	: Regenerating on Windows
