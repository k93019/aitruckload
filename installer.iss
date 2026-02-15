[Setup]
AppName=Truck load finder
AppVersion=1.0.0
AppPublisher=Lighthouse Labs
DefaultDirName={pf}\Truck Load Finder
DefaultGroupName=Truck Load Finder
OutputDir=dist_installer
OutputBaseFilename=TruckLoadFinderSetup
SetupIconFile=Resources\truck_loads_icon.ico
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
