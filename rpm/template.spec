Name:           ros-indigo-lama-core
Version:        0.1.1
Release:        1%{?dist}
Summary:        ROS lama_core package

Group:          Development/Libraries
License:        BSD
URL:            http://wiki.ros.org/Large%20Maps%20Framework
Source0:        %{name}-%{version}.tar.gz

BuildArch:      noarch

Requires:       ros-indigo-lama-interfaces
Requires:       ros-indigo-lama-jockeys
Requires:       ros-indigo-lama-msgs
BuildRequires:  ros-indigo-catkin

%description
Framework for robot autonomy in Large Maps (LaMa), core functionalities

%prep
%setup -q

%build
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree that was dropped by catkin, and source it.  It will
# set things like CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/indigo/setup.sh" ]; then . "/opt/ros/indigo/setup.sh"; fi
mkdir -p obj-%{_target_platform} && cd obj-%{_target_platform}
%cmake .. \
        -UINCLUDE_INSTALL_DIR \
        -ULIB_INSTALL_DIR \
        -USYSCONF_INSTALL_DIR \
        -USHARE_INSTALL_PREFIX \
        -ULIB_SUFFIX \
        -DCMAKE_INSTALL_PREFIX="/opt/ros/indigo" \
        -DCMAKE_PREFIX_PATH="/opt/ros/indigo" \
        -DSETUPTOOLS_DEB_LAYOUT=OFF \
        -DCATKIN_BUILD_BINARY_PACKAGE="1" \

make %{?_smp_mflags}

%install
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree that was dropped by catkin, and source it.  It will
# set things like CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/indigo/setup.sh" ]; then . "/opt/ros/indigo/setup.sh"; fi
cd obj-%{_target_platform}
make %{?_smp_mflags} install DESTDIR=%{buildroot}

%files
/opt/ros/indigo

%changelog
* Thu Jan 08 2015 Gaël Ecorchard <gael.ecorchard@ciirc.cvut.cz> - 0.1.1-1
- Autogenerated by Bloom

* Thu Jan 08 2015 Gaël Ecorchard <gael.ecorchard@ciirc.cvut.cz> - 0.1.1-0
- Autogenerated by Bloom

* Wed Jan 07 2015 Gaël Ecorchard <gael.ecorchard@ciirc.cvut.cz> - 0.1.0-1
- Autogenerated by Bloom

* Wed Jan 07 2015 Gaël Ecorchard <gael.ecorchard@ciirc.cvut.cz> - 0.1.0-0
- Autogenerated by Bloom

