#
# Cookbook Name:: sql_helper
# File:: os_modules
#
# Copyright 2016, Alex Munoz
#
# All rights reserved - Do Not Redistribute
#
# sql_helper cookbook

# Provides windows configuration checks and tools
module WindowsConfiguration
  # Check if node is on 2012r2 or above
  def windows_2012r2_or_above(platform_version = 0)
    platform_version = node['platform_version'] if platform_version == 0
    Gem::Version.new(platform_version) >= Gem::Version.new('6.3.9600')
  end

  # Currently only supports 4.5.X and 4.6.X
  def dotnet_version_is_installed(version)
    pscommand = <<-EOS
      $framework_version = '#{version}'
      !!(Get-ChildItem 'HKLM:\\SOFTWARE\\Microsoft\\NET Framework Setup\\NDP' -recurse |
        Get-ItemProperty -name Version,Release -EA 0 |
        Where { $_.PSChildName -match '^(?!S)\\p{L}'} |
        Select @{
        name="Product"
        expression={
          switch($_.Release) {
            378389 { [Version]"4.5" }
            378675 { [Version]"4.5.1" }
            378758 { [Version]"4.5.1" }
            379893 { [Version]"4.5.2" }
            393295 { [Version]"4.6" }
            393297 { [Version]"4.6" }
            394254 { [Version]"4.6.1" }
            394271 { [Version]"4.6.1" }
            394802 { [Version]"4.6.2" }
            394806 { [Version]"4.6.2" }
          }
        }
      } |
      Findstr /c:"$framework_version")
    EOS
    powershell_out(pscommand)
  end

  # Get a windows-friendly filename
  def windows_friendly_filename(filename)
    filename.strip.gsub(%r{[\x00\/\\:\*\?\"<>\|]}, '_')
  end

  # Get a windows-friendly folder path
  def windows_friendly_directory_path(directory)
    directory.strip.gsub(/[\x00\*\?\"<>\|]/, '_')
  end

  if Chef::Platform.windows? && !defined? GetDiskFreeSpaceEx
    require 'Win32API'
    GetDiskFreeSpaceEx = Win32API.new('kernel32', 'GetDiskFreeSpaceEx', 'PPPP', 'I')
  end

  def get_disk_free_space(path)
    raise 'Cannot check free space for path provided. Path is empty or nil.' if path.nil? || path.empty? || path == 'null'
    root_folder = get_root_directory(path)

    raise "Cannot check free space for #{path} - The path was not found." if root_folder.nil? || root_folder.empty?
    root_folder = ensure_closing_slash(root_folder)

    free = [0].pack('Q')
    GetDiskFreeSpaceEx.call(root_folder, 0, 0, free)
    free = free.unpack('Q').first

    (free / 1024.0 / 1024.0).round(2)
  end

  def get_disk_size(path)
    raise 'Cannot check free space for path provided. Path is empty or nil.' if path.nil? || path.empty? || path == 'null'
    root_folder = get_root_directory(path)

    raise "Cannot check free space for #{path} - The path was not found." if root_folder.nil? || root_folder.empty?
    root_folder = ensure_closing_slash(root_folder)

    total = [0].pack('Q')
    GetDiskFreeSpaceEx.call(root_folder, 0, total, 0)
    total = total.unpack('Q').first

    (total / 1024.0 / 1024.0).round(2)
  end

  # Gets the root directory of a path. Local and UNC paths accepted
  def get_root_directory(path)
    computed_path = path
    computed_path = File.dirname(computed_path) while computed_path != File.dirname(computed_path)
    computed_path
  end

  # Ensures that the path ends with a slash. Only directories should be passed
  def ensure_closing_slash(path)
    path.nil? || path.empty? || path[-1] == '\\' ? path : "#{path}\\"
  end
end
