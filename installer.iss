#ifndef AppVersion
  #define AppVersion "1.0.19"
#endif

[Setup]
AppName=Truck load finder
AppVersion={#AppVersion}
AppPublisher=Lighthouse Labs
AppId={{9F3D6D8E-5C74-4F0A-9B70-7E2C3E3E6D9B}}
DefaultDirName={pf}\Truck Load Finder
DefaultGroupName=Truck Load Finder
OutputDir=dist_installer
OutputBaseFilename=TruckLoadFinderSetup-{#AppVersion}
SetupIconFile=src\assets\icons\truck_loads_icon.ico
UninstallDisplayIcon={app}\Truck Load Finder.exe
WizardStyle=modern
PrivilegesRequired=admin

[Files]
Source: "dist\Truck Load Finder.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Truck Load Finder"; Filename: "{app}\Truck Load Finder.exe"; IconFilename: "{app}\Truck Load Finder.exe"
Name: "{commondesktop}\Truck Load Finder"; Filename: "{app}\Truck Load Finder.exe"; IconFilename: "{app}\Truck Load Finder.exe"

[Run]
Filename: "{app}\Truck Load Finder.exe"; Description: "Launch Truck load finder"; Flags: nowait postinstall skipifsilent
